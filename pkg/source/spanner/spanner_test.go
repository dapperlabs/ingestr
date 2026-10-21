package spanner

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/civil"
	"cloud.google.com/go/spanner"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/bruin-data/ingestr/pkg/arrowconv"
	"github.com/bruin-data/ingestr/pkg/schema"
	"github.com/bruin-data/ingestr/pkg/source"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseURI(t *testing.T) {
	tests := []struct {
		name       string
		uri        string
		wantDBPath string
		wantErr    bool
		errContain string
	}{
		{
			name:       "valid URI with all params",
			uri:        "spanner://?project_id=my-project&instance_id=my-instance&database=my-db",
			wantDBPath: "projects/my-project/instances/my-instance/databases/my-db",
		},
		{
			name:       "valid URI with credentials_path",
			uri:        "spanner://?project_id=proj&instance_id=inst&database=db&credentials_path=/path/to/creds.json",
			wantDBPath: "projects/proj/instances/inst/databases/db",
		},
		{
			name:       "valid URI with credentials_base64",
			uri:        "spanner://?project_id=proj&instance_id=inst&database=db&credentials_base64=" + base64.StdEncoding.EncodeToString([]byte(`{"type":"service_account"}`)),
			wantDBPath: "projects/proj/instances/inst/databases/db",
		},
		{
			name:       "missing project_id",
			uri:        "spanner://?instance_id=inst&database=db",
			wantErr:    true,
			errContain: "project_id, instance_id, and database are required",
		},
		{
			name:       "missing instance_id",
			uri:        "spanner://?project_id=proj&database=db",
			wantErr:    true,
			errContain: "project_id, instance_id, and database are required",
		},
		{
			name:       "missing database",
			uri:        "spanner://?project_id=proj&instance_id=inst",
			wantErr:    true,
			errContain: "project_id, instance_id, and database are required",
		},
		{
			name:       "empty URI",
			uri:        "spanner://",
			wantErr:    true,
			errContain: "project_id, instance_id, and database are required",
		},
		{
			name:       "invalid base64 credentials",
			uri:        "spanner://?project_id=proj&instance_id=inst&database=db&credentials_base64=not-valid-base64!!!",
			wantErr:    true,
			errContain: "failed to decode credentials_base64",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dbPath, opts, err := parseURI(tt.uri)
			if tt.wantErr {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.errContain)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantDBPath, dbPath)
			if tt.uri == "spanner://?project_id=my-project&instance_id=my-instance&database=my-db" {
				assert.Empty(t, opts, "no credentials means no client options")
			}
		})
	}
}

func TestMapSpannerTypeString(t *testing.T) {
	tests := []struct {
		name      string
		input     string
		wantType  schema.DataType
		wantPrec  int
		wantScale int
		wantArray schema.DataType
	}{
		{"BOOL", "BOOL", schema.TypeBoolean, 0, 0, schema.TypeUnknown},
		{"INT64", "INT64", schema.TypeInt64, 0, 0, schema.TypeUnknown},
		{"FLOAT32", "FLOAT32", schema.TypeFloat32, 0, 0, schema.TypeUnknown},
		{"FLOAT64", "FLOAT64", schema.TypeFloat64, 0, 0, schema.TypeUnknown},
		{"NUMERIC", "NUMERIC", schema.TypeDecimal, 38, 9, schema.TypeUnknown},
		{"STRING", "STRING(MAX)", schema.TypeString, 0, 0, schema.TypeUnknown},
		{"STRING with length", "STRING(255)", schema.TypeString, 0, 0, schema.TypeUnknown},
		{"BYTES", "BYTES(MAX)", schema.TypeBinary, 0, 0, schema.TypeUnknown},
		{"DATE", "DATE", schema.TypeDate, 0, 0, schema.TypeUnknown},
		{"TIMESTAMP", "TIMESTAMP", schema.TypeTimestampTZ, 0, 0, schema.TypeUnknown},
		{"JSON", "JSON", schema.TypeJSON, 0, 0, schema.TypeUnknown},
		{"ARRAY<STRING>", "ARRAY<STRING(MAX)>", schema.TypeArray, 0, 0, schema.TypeString},
		{"ARRAY<INT64>", "ARRAY<INT64>", schema.TypeArray, 0, 0, schema.TypeInt64},
		{"ARRAY<FLOAT64>", "ARRAY<FLOAT64>", schema.TypeArray, 0, 0, schema.TypeFloat64},
		{"ARRAY<BOOL>", "ARRAY<BOOL>", schema.TypeArray, 0, 0, schema.TypeBoolean},
		{"lowercase", "bool", schema.TypeBoolean, 0, 0, schema.TypeUnknown},
		{"mixed case", "String(MAX)", schema.TypeString, 0, 0, schema.TypeUnknown},
		{"unknown type", "PROTO", schema.TypeString, 0, 0, schema.TypeUnknown},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt, p, s, at := mapSpannerTypeString(tt.input)
			assert.Equal(t, tt.wantType, dt, "DataType")
			assert.Equal(t, tt.wantPrec, p, "Precision")
			assert.Equal(t, tt.wantScale, s, "Scale")
			assert.Equal(t, tt.wantArray, at, "ArrayType")
		})
	}
}

func TestFilterColumns(t *testing.T) {
	columns := []schema.Column{
		{Name: "id"},
		{Name: "name"},
		{Name: "email"},
		{Name: "created_at"},
	}

	tests := []struct {
		name    string
		exclude []string
		want    []string
	}{
		{
			name:    "no exclusions",
			exclude: nil,
			want:    []string{"id", "name", "email", "created_at"},
		},
		{
			name:    "exclude one column",
			exclude: []string{"email"},
			want:    []string{"id", "name", "created_at"},
		},
		{
			name:    "exclude multiple columns",
			exclude: []string{"email", "created_at"},
			want:    []string{"id", "name"},
		},
		{
			name:    "exclude with different case",
			exclude: []string{"EMAIL", "Name"},
			want:    []string{"id", "created_at"},
		},
		{
			name:    "exclude non-existent column",
			exclude: []string{"nonexistent"},
			want:    []string{"id", "name", "email", "created_at"},
		},
		{
			name:    "empty exclude list",
			exclude: []string{},
			want:    []string{"id", "name", "email", "created_at"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := filterColumns(columns, tt.exclude)
			got := make([]string, len(result))
			for i, c := range result {
				got[i] = c.Name
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBuildSelectQuery(t *testing.T) {
	columns := []schema.Column{
		{Name: "id", DataType: schema.TypeInt64},
		{Name: "name", DataType: schema.TypeString},
		{Name: "created_at", DataType: schema.TypeTimestampTZ},
	}

	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC)

	tests := []struct {
		name  string
		table string
		opts  source.ReadOptions
		want  string
	}{
		{
			name:  "basic select",
			table: "users",
			opts:  source.ReadOptions{},
			want:  "SELECT `id`, `name`, `created_at` FROM `users`",
		},
		{
			name:  "with limit",
			table: "users",
			opts:  source.ReadOptions{Limit: 10},
			want:  "SELECT `id`, `name`, `created_at` FROM `users` LIMIT 10",
		},
		{
			name:  "with incremental key and both intervals",
			table: "events",
			opts: source.ReadOptions{
				IncrementalKey: "created_at",
				IntervalStart:  &start,
				IntervalEnd:    &end,
			},
			want: "SELECT `id`, `name`, `created_at` FROM `events` WHERE `created_at` >= TIMESTAMP('2024-01-01T00:00:00Z') AND `created_at` <= TIMESTAMP('2024-12-31T23:59:59Z')",
		},
		{
			name:  "with incremental key start only",
			table: "events",
			opts: source.ReadOptions{
				IncrementalKey: "created_at",
				IntervalStart:  &start,
			},
			want: "SELECT `id`, `name`, `created_at` FROM `events` WHERE `created_at` >= TIMESTAMP('2024-01-01T00:00:00Z')",
		},
		{
			name:  "with incremental key end only",
			table: "events",
			opts: source.ReadOptions{
				IncrementalKey: "created_at",
				IntervalEnd:    &end,
			},
			want: "SELECT `id`, `name`, `created_at` FROM `events` WHERE `created_at` <= TIMESTAMP('2024-12-31T23:59:59Z')",
		},
		{
			name:  "incremental key with limit",
			table: "events",
			opts: source.ReadOptions{
				IncrementalKey: "created_at",
				IntervalStart:  &start,
				Limit:          100,
			},
			want: "SELECT `id`, `name`, `created_at` FROM `events` WHERE `created_at` >= TIMESTAMP('2024-01-01T00:00:00Z') LIMIT 100",
		},
		{
			name:  "no incremental key ignores intervals",
			table: "events",
			opts: source.ReadOptions{
				IntervalStart: &start,
				IntervalEnd:   &end,
			},
			want: "SELECT `id`, `name`, `created_at` FROM `events`",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildSelectQuery(tt.table, columns, tt.opts)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBuildArrowSchema(t *testing.T) {
	columns := []schema.Column{
		{Name: "id", DataType: schema.TypeInt64, Nullable: false},
		{Name: "name", DataType: schema.TypeString, Nullable: true},
		{Name: "score", DataType: schema.TypeFloat64, Nullable: true},
		{Name: "active", DataType: schema.TypeBoolean, Nullable: false},
	}

	arrowSchema := buildArrowSchema(columns)

	require.Equal(t, 4, arrowSchema.NumFields())
	assert.Equal(t, "id", arrowSchema.Field(0).Name)
	assert.Equal(t, "name", arrowSchema.Field(1).Name)
	assert.Equal(t, "score", arrowSchema.Field(2).Name)
	assert.Equal(t, "active", arrowSchema.Field(3).Name)
	assert.False(t, arrowSchema.Field(0).Nullable)
	assert.True(t, arrowSchema.Field(1).Nullable)
}


func TestExtractArrayValueNullStaysNil(t *testing.T) {
	// A NULL Spanner array must reach arrowconv.AppendValue as an untyped nil.
	// Regression: arrayValues returned []E, so a nil slice boxed into
	// interface{} was non-nil, AppendValue took the append path, and NULL
	// arrays landed in Postgres as empty arrays — "not configured" and
	// "configured empty" collapsed into one value.
	nullStringArray := columnGetterFunc(func(idx int, ptr interface{}) error {
		p, ok := ptr.(*[]spanner.NullString)
		if !ok {
			return fmt.Errorf("unexpected decode target %T", ptr)
		}
		*p = nil // NULL array
		return nil
	})

	val, err := extractArrayValue(nullStringArray, 0, schema.Column{Name: "Tags", ArrayType: schema.TypeString})
	if err != nil {
		t.Fatalf("extractArrayValue: %v", err)
	}
	if val != nil {
		t.Fatalf("NULL array decoded to non-nil interface %T (%v); AppendValue would write an empty list", val, val)
	}

	// And the empty array must stay distinguishable from NULL.
	emptyStringArray := columnGetterFunc(func(idx int, ptr interface{}) error {
		p := ptr.(*[]spanner.NullString)
		*p = []spanner.NullString{}
		return nil
	})
	val, err = extractArrayValue(emptyStringArray, 0, schema.Column{Name: "Tags", ArrayType: schema.TypeString})
	if err != nil {
		t.Fatalf("extractArrayValue: %v", err)
	}
	got, ok := val.([]string)
	if !ok {
		t.Fatalf("empty array decoded to %T, want []string", val)
	}
	if got == nil || len(got) != 0 {
		t.Fatalf("empty array decoded to %#v, want non-nil zero-length slice", got)
	}
}

type columnGetterFunc func(idx int, ptr interface{}) error

func (f columnGetterFunc) Column(idx int, ptr interface{}) error { return f(idx, ptr) }

func TestSchemes(t *testing.T) {
	s := NewSpannerSource()
	assert.Equal(t, []string{"spanner"}, s.Schemes())
}

func TestHandlesIncrementality(t *testing.T) {
	s := NewSpannerSource()
	assert.False(t, s.HandlesIncrementality())
}

func TestNewSpannerSource(t *testing.T) {
	s := NewSpannerSource()
	require.NotNil(t, s)
	assert.Nil(t, s.client)
	assert.Empty(t, s.dbPath)
}

// *spanner.Row must satisfy columnGetter, otherwise the seam the tests below
// rely on would not exist in production.
var _ columnGetter = (*spanner.Row)(nil)

// stubColumnRow stands in for *spanner.Row, handing out the value registered
// for a column index without a live Spanner client. It rejects a decode whose
// destination type does not match the registered value, the same way the real
// client rejects a destination slice whose element type does not match the
// column.
type stubColumnRow struct {
	values []interface{}
}

func (s stubColumnRow) Column(idx int, ptr interface{}) error {
	target := reflect.ValueOf(ptr)
	if target.Kind() != reflect.Pointer || target.IsNil() {
		return fmt.Errorf("stubColumnRow: destination for column %d must be a non-nil pointer", idx)
	}
	if idx >= len(s.values) {
		return fmt.Errorf("stubColumnRow: no value registered for column %d", idx)
	}
	element := target.Elem()
	value := s.values[idx]
	if value == nil {
		element.Set(reflect.Zero(element.Type()))
		return nil
	}
	source := reflect.ValueOf(value)
	if !source.Type().AssignableTo(element.Type()) {
		return fmt.Errorf("stubColumnRow: column %d holds %s, cannot decode into %s", idx, source.Type(), element.Type())
	}
	element.Set(source)
	return nil
}

func mustRat(t *testing.T, literal string) *big.Rat {
	t.Helper()
	rat, ok := new(big.Rat).SetString(literal)
	require.True(t, ok, "invalid decimal literal %q", literal)
	return rat
}

func numericArrayColumn() schema.Column {
	return schema.Column{
		Name:      "prices",
		DataType:  schema.TypeArray,
		ArrayType: schema.TypeDecimal,
		Precision: 38,
		Scale:     9,
	}
}

// ARRAY<NUMERIC> values must come out as exact decimals. The literals below
// cannot survive a float64 round-trip, so a float64 conversion shows up as a
// changed digit rather than as a passing test.
func TestExtractColumnValueArrayNumericIsExact(t *testing.T) {
	row := stubColumnRow{values: []interface{}{
		[]spanner.NullNumeric{
			{Numeric: *mustRat(t, "12345678901234.567890123"), Valid: true},
			{Numeric: *mustRat(t, "0.1"), Valid: true},
			{Numeric: *mustRat(t, "-0.000000001"), Valid: true},
			{Numeric: *mustRat(t, "9007199254740993.5"), Valid: true},
			{Valid: false},
		},
	}}

	got, err := extractColumnValue(row, 0, numericArrayColumn())
	require.NoError(t, err)

	values, ok := got.([]string)
	require.Truef(t, ok, "ARRAY<NUMERIC> must be carried as exact decimal strings, got %T", got)
	assert.Equal(t, []string{
		"12345678901234.567890123",
		"0.100000000",
		"-0.000000001",
		"9007199254740993.500000000",
	}, values)
}

// The exact decimals must survive the Arrow conversion the reader actually
// performs, not only the type switch.
func TestArrayNumericSurvivesArrowBatchConversion(t *testing.T) {
	col := numericArrayColumn()
	arrowSchema := buildArrowSchema([]schema.Column{col})
	require.Equal(t, arrow.ListOf(&arrow.Decimal128Type{Precision: 38, Scale: 9}), arrowSchema.Field(0).Type)

	row := stubColumnRow{values: []interface{}{
		[]spanner.NullNumeric{
			{Numeric: *mustRat(t, "12345678901234.567890123"), Valid: true},
			{Numeric: *mustRat(t, "-0.000000001"), Valid: true},
			{Valid: false},
		},
	}}

	value, err := extractColumnValue(row, 0, col)
	require.NoError(t, err)

	builder := array.NewBuilder(memory.NewGoAllocator(), arrowSchema.Field(0).Type)
	defer builder.Release()
	arrowconv.AppendValue(builder, value)

	list, ok := builder.NewArray().(*array.List)
	require.True(t, ok)
	defer list.Release()
	require.Equal(t, 1, list.Len())
	require.False(t, list.IsNull(0))

	decimals, ok := list.ListValues().(*array.Decimal128)
	require.True(t, ok)
	require.Equal(t, 2, decimals.Len())

	assert.Equal(t, "12345678901234.567890123", decimals.Value(0).ToString(9))
	assert.Equal(t, "-0.000000001", decimals.Value(1).ToString(9))
}

// Every element type the Spanner client can report for an array must be
// converted rather than silently dropped.
func TestExtractColumnValueArrayElementTypes(t *testing.T) {
	timestamp := time.Date(2026, time.October, 20, 13, 45, 30, 123456000, time.UTC)
	day := civil.Date{Year: 2026, Month: time.October, Day: 20}

	tests := []struct {
		name      string
		arrayType schema.DataType
		value     interface{}
		want      interface{}
	}{
		{
			name:      "string",
			arrayType: schema.TypeString,
			value:     []spanner.NullString{{StringVal: "a", Valid: true}, {Valid: false}, {StringVal: "b", Valid: true}},
			want:      []string{"a", "b"},
		},
		{
			name:      "int64",
			arrayType: schema.TypeInt64,
			value:     []spanner.NullInt64{{Int64: 7, Valid: true}},
			want:      []int64{7},
		},
		{
			name:      "float32",
			arrayType: schema.TypeFloat32,
			value:     []spanner.NullFloat32{{Float32: 1.5, Valid: true}},
			want:      []float32{1.5},
		},
		{
			name:      "float64",
			arrayType: schema.TypeFloat64,
			value:     []spanner.NullFloat64{{Float64: 2.25, Valid: true}},
			want:      []float64{2.25},
		},
		{
			name:      "boolean",
			arrayType: schema.TypeBoolean,
			value:     []spanner.NullBool{{Bool: true, Valid: true}},
			want:      []bool{true},
		},
		{
			name:      "date",
			arrayType: schema.TypeDate,
			value:     []spanner.NullDate{{Date: day, Valid: true}},
			want:      []time.Time{time.Date(2026, time.October, 20, 0, 0, 0, 0, time.UTC)},
		},
		{
			name:      "timestamp",
			arrayType: schema.TypeTimestampTZ,
			value:     []spanner.NullTime{{Time: timestamp, Valid: true}},
			want:      []time.Time{timestamp},
		},
		{
			name:      "bytes",
			arrayType: schema.TypeBinary,
			value:     [][]byte{[]byte("ab")},
			want:      [][]byte{[]byte("ab")},
		},
		{
			name:      "json",
			arrayType: schema.TypeJSON,
			value:     []spanner.NullJSON{{Value: map[string]interface{}{"a": float64(1)}, Valid: true}},
			want:      []string{`{"a":1}`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			col := schema.Column{Name: "items", DataType: schema.TypeArray, ArrayType: test.arrayType}
			row := stubColumnRow{values: []interface{}{test.value}}

			got, err := extractColumnValue(row, 0, col)
			require.NoError(t, err)
			assert.Equal(t, test.want, got)
		})
	}
}

// An array element type that cannot be represented must fail the read. The
// previous behaviour returned NULL, which loses the column without a trace.
func TestExtractColumnValueUnsupportedArrayElementErrors(t *testing.T) {
	for _, arrayType := range []schema.DataType{schema.TypeTime, schema.TypeInterval, schema.TypeArray, schema.TypeUnknown} {
		t.Run(arrayType.String(), func(t *testing.T) {
			col := schema.Column{Name: "unsupported", DataType: schema.TypeArray, ArrayType: arrayType}
			row := stubColumnRow{values: []interface{}{[]spanner.NullString{{StringVal: "x", Valid: true}}}}

			got, err := extractColumnValue(row, 0, col)
			require.Error(t, err)
			assert.Nil(t, got)
			assert.Contains(t, err.Error(), `"unsupported"`)
			assert.Contains(t, err.Error(), arrayType.String())
		})
	}
}

// A decode failure on an array column is reported, not turned into a NULL.
func TestExtractColumnValueArrayDecodeFailureIsReported(t *testing.T) {
	col := numericArrayColumn()
	// A decimal array decoded as strings is exactly the mismatch that used to
	// produce a silent NULL.
	row := stubColumnRow{values: []interface{}{[]spanner.NullString{{StringVal: "1", Valid: true}}}}

	got, err := extractColumnValue(row, 0, col)
	require.Error(t, err)
	assert.Nil(t, got)
	assert.Contains(t, err.Error(), `"prices"`)
}

func TestArrayValuesDropsNullElements(t *testing.T) {
	col := schema.Column{Name: "tags", DataType: schema.TypeArray, ArrayType: schema.TypeString}
	row := stubColumnRow{values: []interface{}{[]spanner.NullString{{Valid: false}, {Valid: false}}}}

	got, err := extractColumnValue(row, 0, col)
	require.NoError(t, err)
	assert.Equal(t, []string{}, got)
}

func TestArrayValuesNilSliceStaysNull(t *testing.T) {
	col := schema.Column{Name: "tags", DataType: schema.TypeArray, ArrayType: schema.TypeString}
	row := stubColumnRow{values: []interface{}{[]spanner.NullString(nil)}}

	got, err := extractColumnValue(row, 0, col)
	require.NoError(t, err)
	assert.Nil(t, got)
}
