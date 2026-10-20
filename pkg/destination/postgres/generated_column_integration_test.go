//go:build integration

package postgres

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/bruin-data/ingestr/pkg/destination"
	"github.com/bruin-data/ingestr/pkg/schema"
	"github.com/bruin-data/ingestr/pkg/source"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

const generatedColumnTableDDL = `(
	id BIGINT PRIMARY KEY,
	quantity BIGINT NOT NULL,
	unit_price NUMERIC(12,2) NOT NULL,
	total_price NUMERIC(14,2) GENERATED ALWAYS AS (quantity * unit_price) STORED
)`

func generatedColumnTableSchema() *schema.TableSchema {
	return &schema.TableSchema{Columns: []schema.Column{
		{Name: "id", DataType: schema.TypeInt64, IsPrimaryKey: true},
		{Name: "quantity", DataType: schema.TypeInt64},
		{Name: "unit_price", DataType: schema.TypeDecimal, Precision: 12, Scale: 2},
		{Name: "total_price", DataType: schema.TypeDecimal, Precision: 14, Scale: 2},
	}}
}

// generatedColumnRecord builds a record whose schema carries the generated
// column too, the way a source such as Spanner supplies it. total_price is
// deliberately written as 0 so a passing assertion proves Postgres computed it.
func generatedColumnRecord(t *testing.T, mem memory.Allocator, ids, quantities []int64, unitPrices []decimal128.Num) arrow.RecordBatch {
	t.Helper()

	idBuilder := array.NewInt64Builder(mem)
	defer idBuilder.Release()
	idBuilder.AppendValues(ids, nil)

	quantityBuilder := array.NewInt64Builder(mem)
	defer quantityBuilder.Release()
	quantityBuilder.AppendValues(quantities, nil)

	unitPriceBuilder := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 12, Scale: 2})
	defer unitPriceBuilder.Release()
	for _, value := range unitPrices {
		unitPriceBuilder.Append(value)
	}

	totalPriceBuilder := array.NewDecimal128Builder(mem, &arrow.Decimal128Type{Precision: 14, Scale: 2})
	defer totalPriceBuilder.Release()
	for range ids {
		totalPriceBuilder.Append(decimal128.FromI64(0))
	}

	// NewRecordBatch retains each array, so the caller keeps and releases its
	// own reference.
	arrays := []arrow.Array{idBuilder.NewArray(), quantityBuilder.NewArray(), unitPriceBuilder.NewArray(), totalPriceBuilder.NewArray()}
	for _, arr := range arrays {
		defer arr.Release()
	}

	return array.NewRecordBatch(
		generatedColumnTableSchema().ToArrowSchema(),
		arrays,
		int64(len(ids)),
	)
}

func startGeneratedColumnPostgres(t *testing.T) *PostgresDestination {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image: "postgres:16-alpine",
			Env: map[string]string{
				"POSTGRES_USER": "testuser", "POSTGRES_PASSWORD": "testpass", "POSTGRES_DB": "testdb",
			},
			ExposedPorts: []string{"5432/tcp"},
			WaitingFor: wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).WithStartupTimeout(time.Minute),
		},
		Started: true,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = container.Terminate(context.Background()) })
	host, err := container.Host(ctx)
	require.NoError(t, err)
	port, err := container.MappedPort(ctx, "5432/tcp")
	require.NoError(t, err)

	dest := NewPostgresDestination()
	uri := fmt.Sprintf("postgres://testuser:testpass@%s:%s/testdb?sslmode=disable", host, port.Port())
	// The container log says "ready" before the published host port is
	// reachable on some Docker backends, so retry until the port accepts.
	deadline := time.Now().Add(30 * time.Second)
	for {
		err := dest.Connect(ctx, uri)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("failed to connect to %s: %v", uri, err)
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Cleanup(func() { _ = dest.Close(context.Background()) })
	return dest
}

func TestGeneratedColumnsAreOmittedFromAppendCopy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	dest := startGeneratedColumnPostgres(t)
	ctx := t.Context()

	require.NoError(t, dest.Exec(ctx, `CREATE TABLE public.generated_orders `+generatedColumnTableDDL))

	tableSchema := generatedColumnTableSchema()
	require.NoError(t, dest.PrepareTable(ctx, destination.PrepareOptions{
		Table:       "public.generated_orders",
		Schema:      tableSchema,
		PrimaryKeys: []string{"id"},
	}))

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	record := generatedColumnRecord(t, mem,
		[]int64{1, 2},
		[]int64{3, 5},
		[]decimal128.Num{decimal128.FromI64(250), decimal128.FromI64(100)},
	)
	records := make(chan source.RecordBatchResult, 1)
	records <- source.RecordBatchResult{Batch: record}
	close(records)

	require.NoError(t, dest.Write(ctx, records, destination.WriteOptions{Table: "public.generated_orders", Schema: tableSchema}))

	var rows int
	require.NoError(t, dest.pool.QueryRow(ctx, `SELECT count(*) FROM public.generated_orders`).Scan(&rows))
	require.Equal(t, 2, rows)

	var totalPrice string
	require.NoError(t, dest.pool.QueryRow(ctx, `SELECT total_price::text FROM public.generated_orders WHERE id = 1`).Scan(&totalPrice))
	require.Equal(t, "7.50", totalPrice)

	require.NoError(t, dest.pool.QueryRow(ctx, `SELECT total_price::text FROM public.generated_orders WHERE id = 2`).Scan(&totalPrice))
	require.Equal(t, "5.00", totalPrice)

	var supplied int
	require.NoError(t, dest.pool.QueryRow(ctx, `SELECT count(*) FROM public.generated_orders WHERE total_price = 0`).Scan(&supplied))
	require.Equal(t, 0, supplied)
}

func TestGeneratedColumnsAreOmittedFromParallelCopy(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	dest := startGeneratedColumnPostgres(t)
	ctx := t.Context()

	require.NoError(t, dest.Exec(ctx, `CREATE TABLE public.generated_parallel_orders `+generatedColumnTableDDL))

	tableSchema := generatedColumnTableSchema()
	require.NoError(t, dest.PrepareTable(ctx, destination.PrepareOptions{
		Table:       "public.generated_parallel_orders",
		Schema:      tableSchema,
		PrimaryKeys: []string{"id"},
	}))

	mem := memory.NewCheckedAllocator(memory.NewGoAllocator())
	t.Cleanup(func() { mem.AssertSize(t, 0) })
	records := make(chan source.RecordBatchResult, 2)
	for _, batch := range [][]int64{{1, 2}, {3, 4}} {
		record := generatedColumnRecord(t, mem, batch,
			[]int64{2, 4},
			[]decimal128.Num{decimal128.FromI64(125), decimal128.FromI64(250)},
		)
		records <- source.RecordBatchResult{Batch: record}
	}
	close(records)

	require.NoError(t, dest.WriteParallel(ctx, records, destination.WriteOptions{
		Table:       "public.generated_parallel_orders",
		Schema:      tableSchema,
		Parallelism: 2,
	}))

	var rows int
	require.NoError(t, dest.pool.QueryRow(ctx, `SELECT count(*) FROM public.generated_parallel_orders`).Scan(&rows))
	require.Equal(t, 4, rows)

	var matching int
	require.NoError(t, dest.pool.QueryRow(ctx, `
		SELECT count(*) FROM public.generated_parallel_orders
		WHERE total_price = quantity * unit_price`).Scan(&matching))
	require.Equal(t, 4, matching)
}

func TestGeneratedColumnsAreOmittedFromMerge(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}
	dest := startGeneratedColumnPostgres(t)
	ctx := t.Context()

	require.NoError(t, dest.Exec(ctx, `
		CREATE SCHEMA _bruin_staging;
		CREATE TABLE public.generated_merge_orders `+generatedColumnTableDDL+`;
		CREATE TABLE _bruin_staging.generated_merge_orders (
			id BIGINT, quantity BIGINT, unit_price NUMERIC(12,2), total_price NUMERIC(14,2)
		);
		INSERT INTO _bruin_staging.generated_merge_orders VALUES (1, 4, 3.00, 0);
	`))

	require.NoError(t, dest.MergeTable(ctx, destination.MergeOptions{
		StagingTable:             "_bruin_staging.generated_merge_orders",
		TargetTable:              "public.generated_merge_orders",
		PrimaryKeys:              []string{"id"},
		Columns:                  []string{"id", "quantity", "unit_price", "total_price"},
		StagingPrimaryKeysUnique: true,
	}))

	var rows int
	require.NoError(t, dest.pool.QueryRow(ctx, `SELECT count(*) FROM public.generated_merge_orders`).Scan(&rows))
	require.Equal(t, 1, rows)

	var totalPrice string
	require.NoError(t, dest.pool.QueryRow(ctx, `SELECT total_price::text FROM public.generated_merge_orders WHERE id = 1`).Scan(&totalPrice))
	require.Equal(t, "12.00", totalPrice)

	// A second merge must update the existing row through the same projection.
	require.NoError(t, dest.Exec(ctx, `UPDATE _bruin_staging.generated_merge_orders SET quantity = 10, total_price = 0 WHERE id = 1`))
	require.NoError(t, dest.MergeTable(ctx, destination.MergeOptions{
		StagingTable:             "_bruin_staging.generated_merge_orders",
		TargetTable:              "public.generated_merge_orders",
		PrimaryKeys:              []string{"id"},
		Columns:                  []string{"id", "quantity", "unit_price", "total_price"},
		StagingPrimaryKeysUnique: true,
	}))

	require.NoError(t, dest.pool.QueryRow(ctx, `SELECT count(*) FROM public.generated_merge_orders`).Scan(&rows))
	require.Equal(t, 1, rows)
	require.NoError(t, dest.pool.QueryRow(ctx, `SELECT total_price::text FROM public.generated_merge_orders WHERE id = 1`).Scan(&totalPrice))
	require.Equal(t, "30.00", totalPrice)
}
