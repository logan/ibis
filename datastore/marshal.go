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

var columnTypeMap = map[string]string{
	"[]byte":    "blob",
	"bool":      "boolean",
	"float64":   "double",
	"int64":     "bigint",
	"string":    "varchar",
	"time.Time": "timestamp",
}

var typeInfoMap = map[string]*gocql.TypeInfo{
	"boolean":   &gocql.TypeInfo{Type: gocql.TypeBoolean},
	"blob":      &gocql.TypeInfo{Type: gocql.TypeBlob},
	"double":    &gocql.TypeInfo{Type: gocql.TypeDouble},
	"bigint":    &gocql.TypeInfo{Type: gocql.TypeBigInt},
	"varchar":   &gocql.TypeInfo{Type: gocql.TypeVarchar},
	"timestamp": &gocql.TypeInfo{Type: gocql.TypeTimestamp},
}

var column_validators = map[string]string{
	"org.apache.cassandra.db.marshal.BooleanType":   "boolean",
	"org.apache.cassandra.db.marshal.BytesType":     "blob",
	"org.apache.cassandra.db.marshal.DoubleType":    "double",
	"org.apache.cassandra.db.marshal.LongType":      "bigint",
	"org.apache.cassandra.db.marshal.TimestampType": "timestamp",
	"org.apache.cassandra.db.marshal.UTF8Type":      "varchar",
}
