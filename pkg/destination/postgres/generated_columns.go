package postgres

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
)

// postgresSchemaInspector is the subset of the pgx connection and transaction
// APIs needed to inspect a target table's generated columns. Both *pgxpool.Conn
// and pgx.Tx satisfy it.
type postgresSchemaInspector interface {
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// generatedColumnCache memoises the generated-column sets discovered on target
// tables for the lifetime of the destination.
type generatedColumnCache struct {
	mu    sync.Mutex
	byKey map[string]map[string]struct{}
}

// generatedColumnSet returns the set of column names on the target table that
// Postgres computes itself (GENERATED ALWAYS AS ... STORED or VIRTUAL). The
// result is memoised for the lifetime of the destination so a run inspects
// pg_attribute at most once per table.
func (d *PostgresDestination) generatedColumnSet(ctx context.Context, inspector postgresSchemaInspector, table string) (map[string]struct{}, error) {
	d.generatedColumns.mu.Lock()
	defer d.generatedColumns.mu.Unlock()

	// The raw target name is checked first so the repeated lookups a batch loop
	// performs cost a map read instead of a catalog round trip.
	if cached, ok := d.generatedColumns.byKey[table]; ok {
		return cached, nil
	}

	schemaName, tableName, err := d.resolveSchemaTable(ctx, inspector, table)
	if err != nil {
		return nil, err
	}

	key := schemaName + "." + tableName
	generated, ok := d.generatedColumns.byKey[key]
	if !ok {
		generated, err = inspectGeneratedColumns(ctx, inspector, schemaName, tableName, table)
		if err != nil {
			return nil, err
		}
		if d.generatedColumns.byKey == nil {
			d.generatedColumns.byKey = make(map[string]map[string]struct{})
		}
		d.generatedColumns.byKey[key] = generated
	}
	d.generatedColumns.byKey[table] = generated
	return generated, nil
}

func inspectGeneratedColumns(ctx context.Context, inspector postgresSchemaInspector, schemaName, tableName, table string) (map[string]struct{}, error) {
	generated := make(map[string]struct{})

	// attgenerated only exists on PostgreSQL 12 and newer.
	var serverVersion int
	if err := inspector.QueryRow(ctx, `SELECT current_setting('server_version_num')::int`).Scan(&serverVersion); err != nil {
		return nil, fmt.Errorf("failed to inspect generated columns for %s: %w", table, err)
	}
	if serverVersion < 120000 {
		return generated, nil
	}

	rows, err := inspector.Query(ctx, `
		SELECT a.attname
		FROM pg_attribute a
		JOIN pg_class c ON c.oid = a.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1 AND c.relname = $2
		  AND a.attnum > 0 AND NOT a.attisdropped AND a.attgenerated <> ''`, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to inspect generated columns for %s: %w", table, err)
	}
	defer rows.Close()

	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, fmt.Errorf("failed to inspect generated columns for %s: %w", table, err)
		}
		generated[column] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to inspect generated columns for %s: %w", table, err)
	}
	return generated, nil
}

// dropGeneratedColumns returns columns without the Postgres-computed ones.
// It never mutates its input. It returns the input unchanged when nothing is
// dropped, so the common case allocates nothing.
func dropGeneratedColumns(columns []string, generated map[string]struct{}) []string {
	if len(generated) == 0 || len(columns) == 0 {
		return columns
	}
	kept := make([]string, 0, len(columns))
	for _, column := range columns {
		if _, ok := generated[column]; ok {
			continue
		}
		kept = append(kept, column)
	}
	if len(kept) == len(columns) {
		return columns
	}
	return kept
}
