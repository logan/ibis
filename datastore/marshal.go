package datastore

import "tux21b.org/v1/gocql"

// RowValue is a marshalled value along with its type info.
type RowValue struct {
	Value    []byte
	TypeInfo *gocql.TypeInfo
}

func (rv RowValue) MarshalCQL(info *gocql.TypeInfo) ([]byte, error) {
	return rv.Value, nil
}

func (rv *RowValue) UnmarshalCQL(info *gocql.TypeInfo, data []byte) error {
	rv.Value = data
	rv.TypeInfo = info
	return nil
}

// RowValues is a map of strings to marshalled values. This represents the fields extracted from an
// instance of a Table, or the values scanned from a CQL query.
type RowValues map[string]RowValue
