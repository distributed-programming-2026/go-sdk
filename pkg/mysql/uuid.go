package mysql

import (
	"database/sql"
	"uuid"
)

func normalizeQueryArgs(args []any) []any {
	var normalized []any
	for i, arg := range args {
		value, changed := normalizeQueryArg(arg)
		if !changed {
			continue
		}
		if normalized == nil {
			normalized = append([]any(nil), args...)
		}
		normalized[i] = value
	}
	if normalized == nil {
		return args
	}
	return normalized
}

func normalizeQueryArg(arg any) (any, bool) {
	switch value := arg.(type) {
	case uuid.UUID:
		return uuidBytes(value), true
	case *uuid.UUID:
		if value == nil {
			return nil, true
		}
		return uuidBytes(*value), true
	case sql.Null[uuid.UUID]:
		if !value.Valid {
			return nil, true
		}
		return uuidBytes(value.V), true
	case *sql.Null[uuid.UUID]:
		if value == nil || !value.Valid {
			return nil, true
		}
		return uuidBytes(value.V), true
	case sql.NamedArg:
		normalized, changed := normalizeQueryArg(value.Value)
		if !changed {
			return arg, false
		}
		value.Value = normalized
		return value, true
	default:
		return arg, false
	}
}

func uuidBytes(id uuid.UUID) []byte {
	value := make([]byte, len(id))
	copy(value, id[:])
	return value
}
