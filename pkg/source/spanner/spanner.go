package spanner

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"log"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/bruin-data/ingestr/internal/config"
	"github.com/bruin-data/ingestr/pkg/arrowconv"
	"github.com/bruin-data/ingestr/pkg/schema"
	"github.com/bruin-data/ingestr/pkg/source"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

var silentLogger = log.New(io.Discard, "", 0)

type SpannerSource struct {
	client *spanner.Client
	dbPath string
}

func NewSpannerSource() *SpannerSource {
	return &SpannerSource{}
}

func (s *SpannerSource) Schemes() []string {
	return []string{"spanner"}
}

func (s *SpannerSource) Connect(ctx context.Context, uri string) error {
	dbPath, opts, err := parseURI(uri)
	if err != nil {
		return err
	}

	client, err := spanner.NewClientWithConfig(ctx, dbPath, spanner.ClientConfig{Logger: silentLogger}, opts...)
	if err != nil {
		return fmt.Errorf("failed to connect to spanner: %w", err)
	}

	s.client = client
	s.dbPath = dbPath
	config.Debug("[SPANNER] Connected to %s", dbPath)
	return nil
}

func (s *SpannerSource) Close(ctx context.Context) error {
	if s.client != nil {
		s.client.Close()
	}
	return nil
}

func (s *SpannerSource) HandlesIncrementality() bool {
	return false
}

func (s *SpannerSource) GetTable(ctx context.Context, req source.TableRequest) (source.SourceTable, error) {
	if req.Name == "" {
		return nil, fmt.Errorf("table name is required")
	}

	tableSchema, err := s.getSchema(ctx, req.Name)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	pks := req.PrimaryKeys
	if len(pks) == 0 {
		pks = tableSchema.PrimaryKeys
	}

	strategy := req.Strategy
	if strategy == "" {
		strategy = config.StrategyReplace
	}

	return &source.DynamicSourceTable{
		TableName:           req.Name,
		TablePrimaryKeys:    pks,
		TableIncrementalKey: req.IncrementalKey,
		TableStrategy:       strategy,
		KnownSchema:         true,
		SchemaFn: func(ctx context.Context) (*schema.TableSchema, error) {
			return tableSchema, nil
		},
		ReadFn: func(ctx context.Context, opts source.ReadOptions) (<-chan source.RecordBatchResult, error) {
			return s.read(ctx, req.Name, tableSchema, opts)
		},
	}, nil
}

func (s *SpannerSource) getSchema(ctx context.Context, table string) (*schema.TableSchema, error) {
	columns, err := s.getSchemaFromQuery(ctx, table)
	if err != nil {
		return nil, err
	}
	if columns == nil {
		columns, err = s.getSchemaFromInfoSchema(ctx, table)
		if err != nil {
			return nil, err
		}
	}

	primaryKeys := s.fetchPrimaryKeys(ctx, table)
	for i := range columns {
		for _, pk := range primaryKeys {
			if columns[i].Name == pk {
				columns[i].IsPrimaryKey = true
				break
			}
		}
	}

	return &schema.TableSchema{
		Name:        table,
		Columns:     columns,
		PrimaryKeys: primaryKeys,
	}, nil
}

func (s *SpannerSource) getSchemaFromQuery(ctx context.Context, table string) ([]schema.Column, error) {
	query := fmt.Sprintf("SELECT * FROM %s LIMIT 1", quoteIdentifier(table))
	iter := s.client.Single().Query(ctx, spanner.NewStatement(query))
	defer iter.Stop()

	row, err := iter.Next()
	if err == iterator.Done {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to query schema: %w", err)
	}

	nullability := s.fetchNullability(ctx, table)

	columns := make([]schema.Column, row.Size())
	for i := 0; i < row.Size(); i++ {
		colName := row.ColumnName(i)
		colType := row.ColumnType(i)
		dt, precision, scale, arrayType := MapSpannerCodeToDataType(colType)

		nullable := true
		if v, ok := nullability[colName]; ok {
			nullable = v
		}

		columns[i] = schema.Column{
			Name:      colName,
			DataType:  dt,
			Nullable:  nullable,
			Precision: precision,
			Scale:     scale,
			ArrayType: arrayType,
		}
	}
	return columns, nil
}

func (s *SpannerSource) getSchemaFromInfoSchema(ctx context.Context, table string) ([]schema.Column, error) {
	query := `SELECT COLUMN_NAME, SPANNER_TYPE, IS_NULLABLE, ORDINAL_POSITION
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_NAME = @table
		ORDER BY ORDINAL_POSITION`

	stmt := spanner.Statement{
		SQL:    query,
		Params: map[string]interface{}{"table": table},
	}

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var columns []schema.Column
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to query INFORMATION_SCHEMA: %w", err)
		}

		var colName, spannerType, isNullable string
		var ordinal int64
		if err := row.Columns(&colName, &spannerType, &isNullable, &ordinal); err != nil {
			return nil, fmt.Errorf("failed to read column info: %w", err)
		}

		dt, precision, scale, arrayType := mapSpannerTypeString(spannerType)
		columns = append(columns, schema.Column{
			Name:      colName,
			DataType:  dt,
			Nullable:  isNullable == "YES",
			Precision: precision,
			Scale:     scale,
			ArrayType: arrayType,
		})
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("table %s not found or has no columns", table)
	}
	return columns, nil
}

func mapSpannerTypeString(spannerType string) (schema.DataType, int, int, schema.DataType) {
	upper := strings.ToUpper(strings.TrimSpace(spannerType))

	if strings.HasPrefix(upper, "ARRAY<") {
		inner := upper[6 : len(upper)-1]
		elemType, precision, scale, _ := mapSpannerTypeString(inner)
		return schema.TypeArray, precision, scale, elemType
	}

	base := upper
	if idx := strings.Index(upper, "("); idx != -1 {
		base = upper[:idx]
	}

	switch base {
	case "BOOL":
		return schema.TypeBoolean, 0, 0, schema.TypeUnknown
	case "INT64":
		return schema.TypeInt64, 0, 0, schema.TypeUnknown
	case "FLOAT32":
		return schema.TypeFloat32, 0, 0, schema.TypeUnknown
	case "FLOAT64":
		return schema.TypeFloat64, 0, 0, schema.TypeUnknown
	case "NUMERIC":
		return schema.TypeDecimal, 38, 9, schema.TypeUnknown
	case "STRING":
		return schema.TypeString, 0, 0, schema.TypeUnknown
	case "BYTES":
		return schema.TypeBinary, 0, 0, schema.TypeUnknown
	case "DATE":
		return schema.TypeDate, 0, 0, schema.TypeUnknown
	case "TIMESTAMP":
		return schema.TypeTimestampTZ, 0, 0, schema.TypeUnknown
	case "JSON":
		return schema.TypeJSON, 0, 0, schema.TypeUnknown
	default:
		return schema.TypeString, 0, 0, schema.TypeUnknown
	}
}

func (s *SpannerSource) fetchNullability(ctx context.Context, table string) map[string]bool {
	query := `SELECT COLUMN_NAME, IS_NULLABLE
		FROM INFORMATION_SCHEMA.COLUMNS
		WHERE TABLE_NAME = @table`

	stmt := spanner.Statement{
		SQL:    query,
		Params: map[string]interface{}{"table": table},
	}

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	result := make(map[string]bool)
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			config.Debug("[SPANNER] Failed to query nullability: %v", err)
			return result
		}

		var colName, isNullable string
		if err := row.Columns(&colName, &isNullable); err == nil {
			result[colName] = isNullable == "YES"
		}
	}
	return result
}

func (s *SpannerSource) fetchPrimaryKeys(ctx context.Context, table string) []string {
	query := `SELECT COLUMN_NAME
		FROM INFORMATION_SCHEMA.INDEX_COLUMNS
		WHERE TABLE_NAME = @table
			AND INDEX_NAME = 'PRIMARY_KEY'
		ORDER BY ORDINAL_POSITION`

	stmt := spanner.Statement{
		SQL:    query,
		Params: map[string]interface{}{"table": table},
	}

	iter := s.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var pks []string
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			config.Debug("[SPANNER] Failed to query primary keys: %v", err)
			return nil
		}

		var colName string
		if err := row.Columns(&colName); err == nil {
			pks = append(pks, colName)
		}
	}

	config.Debug("[SPANNER] Detected primary keys: %v", pks)
	return pks
}

func (s *SpannerSource) read(ctx context.Context, table string, tableSchema *schema.TableSchema, opts source.ReadOptions) (<-chan source.RecordBatchResult, error) {
	startTotal := time.Now()
	config.Debug("[SPANNER] Starting read from %s", table)

	schemaToUse := tableSchema
	if opts.Schema != nil {
		schemaToUse = opts.Schema
	}

	columns := filterColumns(schemaToUse.Columns, opts.ExcludeColumns)
	arrowSchema := buildArrowSchema(columns)

	batchSize := opts.PageSize
	if batchSize <= 0 {
		batchSize = 100000
	}

	results := make(chan source.RecordBatchResult, 8)

	go func() {
		defer close(results)

		query := buildSelectQuery(table, columns, opts)
		config.Debug("[SPANNER] Executing query: %s", query)

		startQuery := time.Now()
		iter := s.client.Single().Query(ctx, spanner.NewStatement(query))
		defer iter.Stop()
		config.Debug("[SPANNER] Query started in %v", time.Since(startQuery))

		batchNum := 0
		totalRows := int64(0)

		for {
			startBatch := time.Now()
			record, count, err := s.rowsToArrowBatch(iter, arrowSchema, columns, batchSize, opts.MaxBatchBytes)
			if err != nil {
				results <- source.RecordBatchResult{Err: err}
				return
			}

			if count == 0 {
				break
			}

			batchNum++
			totalRows += count
			config.Debug("[SPANNER] Batch %d: %d rows read in %v (total: %d)", batchNum, count, time.Since(startBatch), totalRows)

			results <- source.RecordBatchResult{Batch: record}
		}

		config.Debug("[SPANNER] Total: %d rows in %d batches, read time: %v", totalRows, batchNum, time.Since(startTotal))
	}()

	return results, nil
}

func (s *SpannerSource) rowsToArrowBatch(iter *spanner.RowIterator, arrowSchema *arrow.Schema, columns []schema.Column, batchSize int, maxBatchBytes int64) (arrow.RecordBatch, int64, error) {
	mem := memory.NewGoAllocator()
	builders := make([]array.Builder, len(columns))
	for i, field := range arrowSchema.Fields() {
		builders[i] = array.NewBuilder(mem, field.Type)
	}

	var rowCount int64
	var accBytes int64
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			for _, b := range builders {
				b.Release()
			}
			return nil, 0, fmt.Errorf("failed to read row: %w", err)
		}

		for i := range columns {
			val, err := extractColumnValue(row, i, columns[i])
			if err != nil {
				for _, b := range builders {
					b.Release()
				}
				return nil, 0, err
			}
			arrowconv.AppendValue(builders[i], val)
			if maxBatchBytes > 0 {
				accBytes += arrowconv.ValueBytes(val)
			}
		}
		rowCount++

		if batchSize > 0 && rowCount >= int64(batchSize) {
			break
		}
		if maxBatchBytes > 0 && accBytes >= maxBatchBytes {
			break
		}
	}

	if rowCount == 0 {
		for _, b := range builders {
			b.Release()
		}
		return nil, 0, nil
	}

	arrays := make([]arrow.Array, len(builders))
	for i, b := range builders {
		arrays[i] = b.NewArray()
		b.Release()
	}

	record := array.NewRecordBatch(arrowSchema, arrays, rowCount)
	for _, arr := range arrays {
		arr.Release()
	}

	return record, rowCount, nil
}

// columnGetter decodes the value of result column idx into ptr. *spanner.Row
// implements it; tests substitute their own implementation.
type columnGetter interface {
	Column(idx int, ptr interface{}) error
}

// spannerNumericScale is the scale Spanner reports for NUMERIC, see
// MapSpannerCodeToDataType. Scalar and array NUMERIC columns are rendered with
// this many fractional digits so a value never changes on the way out.
const spannerNumericScale = 9

// arrayValues decodes an ARRAY column into a slice of converted elements,
// skipping SQL NULL elements. Every supported array element type shares this
// shape, and a decode failure is returned as an error: the Spanner client
// rejects a destination slice whose element type does not match the column, and
// that rejection must never be reported as a NULL column.
// The return is `any`, not []E: a typed nil slice boxed into interface{} is
// non-nil, so arrowconv.AppendValue would take the append path and write an
// empty list where Spanner said NULL. NULL and empty are different facts
// ("not configured" vs "configured empty"); every element-type case funnels
// through here, so the boundary is the one place that must get this right.
func arrayValues[V any, E any](row columnGetter, idx int, col schema.Column, convert func(V) (E, bool)) (any, error) {
	var v []V
	if err := row.Column(idx, &v); err != nil {
		return nil, fmt.Errorf("failed to read ARRAY<%s> column %q: %w", col.ArrayType, col.Name, err)
	}
	if v == nil {
		return nil, nil
	}
	result := make([]E, 0, len(v))
	for i, element := range v {
		value, ok := convert(element)
		if !ok {
			// Fail loudly: silently skipping a NULL element shortens the
			// array — same class of data loss as the NULL-array boxing this
			// boundary already had. Positions matter in the atlas schema.
			return nil, fmt.Errorf("ARRAY<%s> column %q has a NULL or unconvertible element at index %d", col.ArrayType, col.Name, i)
		}
		result = append(result, value)
	}
	return result, nil
}

func extractColumnValue(row columnGetter, idx int, col schema.Column) (interface{}, error) {
	switch col.DataType {
	case schema.TypeBoolean:
		var v spanner.NullBool
		if err := row.Column(idx, &v); err != nil || !v.Valid {
			return nil, nil
		}
		return v.Bool, nil
	case schema.TypeInt64:
		var v spanner.NullInt64
		if err := row.Column(idx, &v); err != nil || !v.Valid {
			return nil, nil
		}
		return v.Int64, nil
	case schema.TypeFloat32:
		var v spanner.NullFloat32
		if err := row.Column(idx, &v); err != nil || !v.Valid {
			return nil, nil
		}
		return v.Float32, nil
	case schema.TypeFloat64:
		var v spanner.NullFloat64
		if err := row.Column(idx, &v); err != nil || !v.Valid {
			return nil, nil
		}
		return v.Float64, nil
	case schema.TypeString:
		var v spanner.NullString
		if err := row.Column(idx, &v); err != nil || !v.Valid {
			return nil, nil
		}
		return v.StringVal, nil
	case schema.TypeBinary:
		var v []byte
		if err := row.Column(idx, &v); err != nil {
			return nil, nil
		}
		if v == nil {
			return nil, nil
		}
		return v, nil
	case schema.TypeDate:
		var v spanner.NullDate
		if err := row.Column(idx, &v); err != nil || !v.Valid {
			return nil, nil
		}
		return v.Date.In(time.UTC), nil
	case schema.TypeTimestampTZ, schema.TypeTimestamp:
		var v spanner.NullTime
		if err := row.Column(idx, &v); err != nil || !v.Valid {
			return nil, nil
		}
		return v.Time, nil
	case schema.TypeDecimal:
		var v spanner.NullNumeric
		if err := row.Column(idx, &v); err != nil || !v.Valid {
			return nil, nil
		}
		return v.Numeric.FloatString(spannerNumericScale), nil
	case schema.TypeJSON:
		var v spanner.NullJSON
		if err := row.Column(idx, &v); err != nil || !v.Valid {
			return nil, nil
		}
		jsonBytes, err := json.Marshal(v.Value)
		if err != nil {
			return fmt.Sprintf("%v", v.Value), nil
		}
		return string(jsonBytes), nil
	case schema.TypeUUID:
		var v spanner.NullString
		if err := row.Column(idx, &v); err != nil || !v.Valid {
			return nil, nil
		}
		return v.StringVal, nil
	case schema.TypeArray:
		return extractArrayValue(row, idx, col)
	default:
		var v spanner.NullString
		if err := row.Column(idx, &v); err != nil || !v.Valid {
			return nil, nil
		}
		return v.StringVal, nil
	}
}

// extractArrayValue converts an ARRAY column. Every element type Spanner
// reports is handled explicitly, and an element type this function cannot
// represent is an error rather than a NULL column: returning NULL here drops
// the caller's data without telling anyone.
func extractArrayValue(row columnGetter, idx int, col schema.Column) (interface{}, error) {
	switch col.ArrayType {
	case schema.TypeString:
		return arrayValues(row, idx, col, func(s spanner.NullString) (string, bool) {
			return s.StringVal, s.Valid
		})
	case schema.TypeInt64:
		return arrayValues(row, idx, col, func(n spanner.NullInt64) (int64, bool) {
			return n.Int64, n.Valid
		})
	case schema.TypeFloat32:
		return arrayValues(row, idx, col, func(n spanner.NullFloat32) (float32, bool) {
			return n.Float32, n.Valid
		})
	case schema.TypeFloat64:
		return arrayValues(row, idx, col, func(n spanner.NullFloat64) (float64, bool) {
			return n.Float64, n.Valid
		})
	case schema.TypeBoolean:
		return arrayValues(row, idx, col, func(b spanner.NullBool) (bool, bool) {
			return b.Bool, b.Valid
		})
	case schema.TypeDecimal:
		// NUMERIC keeps its exact decimal form; a float64 round-trip would
		// lose digits that matter for money.
		return arrayValues(row, idx, col, func(n spanner.NullNumeric) (string, bool) {
			if !n.Valid {
				return "", false
			}
			return n.Numeric.FloatString(spannerNumericScale), true
		})
	case schema.TypeDate:
		return arrayValues(row, idx, col, func(d spanner.NullDate) (time.Time, bool) {
			if !d.Valid {
				return time.Time{}, false
			}
			return d.Date.In(time.UTC), true
		})
	case schema.TypeTimestamp, schema.TypeTimestampTZ:
		return arrayValues(row, idx, col, func(t spanner.NullTime) (time.Time, bool) {
			return t.Time, t.Valid
		})
	case schema.TypeBinary:
		return arrayValues(row, idx, col, func(b []byte) ([]byte, bool) {
			return b, b != nil
		})
	case schema.TypeJSON:
		return arrayValues(row, idx, col, func(j spanner.NullJSON) (string, bool) {
			if !j.Valid {
				return "", false
			}
			jsonBytes, err := json.Marshal(j.Value)
			if err != nil {
				return fmt.Sprintf("%v", j.Value), true
			}
			return string(jsonBytes), true
		})
	default:
		return nil, fmt.Errorf("cannot convert column %q to an Arrow value: unsupported Spanner array element type %s", col.Name, col.ArrayType)
	}
}

func (s *SpannerSource) ExecuteCustomQuery(ctx context.Context, query string, opts source.ReadOptions) (<-chan source.RecordBatchResult, error) {
	batchSize := opts.PageSize
	if batchSize <= 0 {
		batchSize = 100000
	}

	results := make(chan source.RecordBatchResult, 8)

	go func() {
		defer close(results)

		config.Debug("[SPANNER] Executing custom query: %s", query)

		// Get schema from INFORMATION_SCHEMA is not possible for custom queries,
		// so we execute the query and infer types from the first row
		iter := s.client.Single().Query(ctx, spanner.NewStatement(query))
		defer iter.Stop()

		// Read the first row to get column types
		firstRow, err := iter.Next()
		if err == iterator.Done {
			return
		}
		if err != nil {
			results <- source.RecordBatchResult{Err: fmt.Errorf("failed to execute custom query: %w", err)}
			return
		}

		columns := make([]schema.Column, firstRow.Size())
		for i := 0; i < firstRow.Size(); i++ {
			colName := firstRow.ColumnName(i)
			colType := firstRow.ColumnType(i)
			dt, precision, scale, arrayType := MapSpannerCodeToDataType(colType)
			columns[i] = schema.Column{
				Name:      colName,
				DataType:  dt,
				Nullable:  true,
				Precision: precision,
				Scale:     scale,
				ArrayType: arrayType,
			}
		}
		arrowSchema := buildArrowSchema(columns)

		mem := memory.NewGoAllocator()
		builders := make([]array.Builder, len(columns))
		for i, field := range arrowSchema.Fields() {
			builders[i] = array.NewBuilder(mem, field.Type)
		}
		defer func() {
			for _, builder := range builders {
				builder.Release()
			}
		}()

		var rowCount int64
		var accBytes int64
		appendRow := func(row *spanner.Row) error {
			for i := range columns {
				val, err := extractColumnValue(row, i, columns[i])
				if err != nil {
					return err
				}
				arrowconv.AppendValue(builders[i], val)
				if opts.MaxBatchBytes > 0 {
					accBytes += arrowconv.ValueBytes(val)
				}
			}
			rowCount++
			return nil
		}
		shouldFlush := func() bool {
			return rowCount >= int64(batchSize) || (opts.MaxBatchBytes > 0 && accBytes >= opts.MaxBatchBytes)
		}
		flush := func() {
			arrays := make([]arrow.Array, len(builders))
			for i, builder := range builders {
				arrays[i] = builder.NewArray()
			}
			record := array.NewRecordBatch(arrowSchema, arrays, rowCount)
			for _, arr := range arrays {
				arr.Release()
			}
			results <- source.RecordBatchResult{Batch: record}

			for i, builder := range builders {
				builder.Release()
				builders[i] = array.NewBuilder(mem, arrowSchema.Field(i).Type)
			}
			rowCount = 0
			accBytes = 0
		}

		if err := appendRow(firstRow); err != nil {
			results <- source.RecordBatchResult{Err: err}
			return
		}
		if shouldFlush() {
			flush()
		}

		for {
			row, err := iter.Next()
			if err == iterator.Done {
				break
			}
			if err != nil {
				results <- source.RecordBatchResult{Err: err}
				return
			}

			if err := appendRow(row); err != nil {
				results <- source.RecordBatchResult{Err: err}
				return
			}
			if shouldFlush() {
				flush()
			}
		}

		if rowCount > 0 {
			flush()
		}
	}()

	return results, nil
}

func parseURI(uri string) (string, []option.ClientOption, error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", nil, fmt.Errorf("invalid Spanner URI: %w", err)
	}

	query := parsed.Query()
	projectID := query.Get("project_id")
	instanceID := query.Get("instance_id")
	database := query.Get("database")

	if projectID == "" || instanceID == "" || database == "" {
		return "", nil, fmt.Errorf("invalid Spanner URI: project_id, instance_id, and database are required, e.g. spanner://?project_id=PROJECT&instance_id=INSTANCE&database=DATABASE")
	}

	dbPath := fmt.Sprintf("projects/%s/instances/%s/databases/%s", projectID, instanceID, database)

	var opts []option.ClientOption
	if credPath := query.Get("credentials_path"); credPath != "" {
		opt, err := credentialsFileOption(credPath)
		if err != nil {
			return "", nil, err
		}
		opts = append(opts, opt)
	} else if credBase64 := query.Get("credentials_base64"); credBase64 != "" {
		credJSON, err := base64.StdEncoding.DecodeString(credBase64)
		if err != nil {
			return "", nil, fmt.Errorf("failed to decode credentials_base64: %w", err)
		}
		opt, err := credentialsJSONOption(credJSON)
		if err != nil {
			return "", nil, err
		}
		opts = append(opts, opt)
	}

	return dbPath, opts, nil
}

// credentialsFileOption builds the client option for a credentials file whose
// type is read from the file itself: service_account keys parse as
// ServiceAccount, external_account (Workload Identity Federation — the EKS
// pilot's shape) as ExternalAccount. Hard-coding ServiceAccount rejects WIF
// configs with a parse error that reads like a permissions problem.
func credentialsFileOption(path string) (option.ClientOption, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("failed to read credentials file: %w", err)
	}
	return credentialsJSONOption(raw)
}

func credentialsJSONOption(raw []byte) (option.ClientOption, error) {
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(raw, &head); err != nil {
		return nil, fmt.Errorf("failed to parse credentials JSON: %w", err)
	}
	switch head.Type {
	case "external_account":
		return option.WithAuthCredentialsJSON(option.ExternalAccount, raw), nil
	case "service_account", "":
		// Empty keeps the historical behaviour for hand-built configs.
		return option.WithAuthCredentialsJSON(option.ServiceAccount, raw), nil
	default:
		return nil, fmt.Errorf("unsupported credentials type %q (want service_account or external_account)", head.Type)
	}
}

func filterColumns(columns []schema.Column, exclude []string) []schema.Column {
	if len(exclude) == 0 {
		return columns
	}

	excludeMap := make(map[string]bool)
	for _, col := range exclude {
		excludeMap[strings.ToLower(col)] = true
	}

	var filtered []schema.Column
	for _, col := range columns {
		if !excludeMap[strings.ToLower(col.Name)] {
			filtered = append(filtered, col)
		}
	}
	return filtered
}

func buildArrowSchema(columns []schema.Column) *arrow.Schema {
	fields := make([]arrow.Field, len(columns))
	for i, col := range columns {
		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     schema.DataTypeToArrowType(col),
			Nullable: col.Nullable,
		}
	}
	return arrow.NewSchema(fields, nil)
}

func quoteIdentifier(name string) string {
	return fmt.Sprintf("`%s`", strings.ReplaceAll(name, "`", "``"))
}

func buildSelectQuery(table string, columns []schema.Column, opts source.ReadOptions) string {
	colNames := make([]string, len(columns))
	for i, col := range columns {
		colNames[i] = quoteIdentifier(col.Name)
	}

	query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(colNames, ", "), quoteIdentifier(table))

	var conditions []string
	if opts.IncrementalKey != "" {
		if opts.IntervalStart != nil {
			conditions = append(conditions, fmt.Sprintf("%s >= TIMESTAMP('%s')", quoteIdentifier(opts.IncrementalKey), opts.IntervalStart.Format("2006-01-02T15:04:05Z")))
		}
		if opts.IntervalEnd != nil {
			conditions = append(conditions, fmt.Sprintf("%s <= TIMESTAMP('%s')", quoteIdentifier(opts.IncrementalKey), opts.IntervalEnd.Format("2006-01-02T15:04:05Z")))
		}
	}

	if len(conditions) > 0 {
		query += " WHERE " + strings.Join(conditions, " AND ")
	}

	if opts.Limit > 0 {
		query += fmt.Sprintf(" LIMIT %d", opts.Limit)
	}

	return query
}

var _ source.Source = (*SpannerSource)(nil)
