package postgres

import (
	"reflect"
	"testing"
)

func TestDropGeneratedColumns(t *testing.T) {
	tests := []struct {
		name      string
		columns   []string
		generated map[string]struct{}
		want      []string
	}{
		{
			name:      "nil generated set returns the input unchanged",
			columns:   []string{"id", "quantity", "total_price"},
			generated: nil,
			want:      []string{"id", "quantity", "total_price"},
		},
		{
			name:      "empty generated set returns the input unchanged",
			columns:   []string{"id", "quantity", "total_price"},
			generated: map[string]struct{}{},
			want:      []string{"id", "quantity", "total_price"},
		},
		{
			name:      "generated set without a match returns the input unchanged",
			columns:   []string{"id", "quantity"},
			generated: map[string]struct{}{"total_price": {}},
			want:      []string{"id", "quantity"},
		},
		{
			name:      "middle column is dropped in order",
			columns:   []string{"id", "quantity", "total_price", "unit_price"},
			generated: map[string]struct{}{"total_price": {}},
			want:      []string{"id", "quantity", "unit_price"},
		},
		{
			name:      "all columns dropped returns empty",
			columns:   []string{"total_price"},
			generated: map[string]struct{}{"total_price": {}},
			want:      []string{},
		},
		{
			name:      "empty column list returns empty",
			columns:   nil,
			generated: map[string]struct{}{"total_price": {}},
			want:      nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			original := append([]string(nil), test.columns...)
			got := dropGeneratedColumns(test.columns, test.generated)
			if !reflect.DeepEqual(got, test.want) {
				t.Fatalf("dropGeneratedColumns(%v) = %v, want %v", test.columns, got, test.want)
			}
			if !reflect.DeepEqual(test.columns, original) {
				t.Fatalf("dropGeneratedColumns mutated its input: %v, want %v", test.columns, original)
			}
		})
	}
}

func TestDropGeneratedColumnsReturnsInputUnchangedWhenNothingIsDropped(t *testing.T) {
	columns := []string{"id", "quantity"}
	got := dropGeneratedColumns(columns, map[string]struct{}{"total_price": {}})
	if &got[0] != &columns[0] {
		t.Fatal("dropGeneratedColumns allocated a new slice although nothing was dropped")
	}
}
