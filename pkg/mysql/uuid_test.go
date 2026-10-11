package mysql

import (
	"database/sql"
	"testing"
	"uuid"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNormalizeQueryArgs(t *testing.T) {
	const unchangedValue = "unchanged"

	id := uuid.MustParse("0199c801-8a6c-7476-b79f-3b9d6f60af7e")
	idPointer := id

	tests := []struct {
		name string
		arg  any
		want any
	}{
		{name: "uuid", arg: id, want: id[:]},
		{name: "uuid pointer", arg: &idPointer, want: id[:]},
		{name: "nil uuid pointer", arg: (*uuid.UUID)(nil), want: nil},
		{name: "valid nullable uuid", arg: sql.Null[uuid.UUID]{V: id, Valid: true}, want: id[:]},
		{name: "null nullable uuid", arg: sql.Null[uuid.UUID]{V: id}, want: nil},
		{name: "valid nullable uuid pointer", arg: &sql.Null[uuid.UUID]{V: id, Valid: true}, want: id[:]},
		{name: "null nullable uuid pointer", arg: &sql.Null[uuid.UUID]{}, want: nil},
		{name: "nil nullable uuid pointer", arg: (*sql.Null[uuid.UUID])(nil), want: nil},
		{name: "named uuid", arg: sql.Named("id", id), want: sql.Named("id", id[:])},
		{name: "other value", arg: unchangedValue, want: unchangedValue},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			args := []any{test.arg}
			got := normalizeQueryArgs(args)
			require.Len(t, got, 1)
			assert.Equal(t, test.want, got[0])
		})
	}
}

func TestNormalizeQueryArgsDoesNotMutateInput(t *testing.T) {
	const value = "value"

	id := uuid.MustParse("0199c801-8a6c-7476-b79f-3b9d6f60af7e")
	args := []any{value, id}

	normalized := normalizeQueryArgs(args)

	assert.Equal(t, []any{value, id}, args)
	assert.Equal(t, []any{value, id[:]}, normalized)
}

func TestNormalizeQueryArgsReusesUnchangedArguments(t *testing.T) {
	const value = "value"

	args := []any{value, 42}

	normalized := normalizeQueryArgs(args)
	normalized[0] = "changed"

	assert.Equal(t, "changed", args[0])
}
