package datastore

import "bytes"
import "errors"
import "reflect"

import "tux21b.org/v1/gocql"

var (
	ErrInvalidRowType = errors.New("row doesn't match schema")
)

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
type RowValues map[string]*RowValue

func (rv *RowValues) subtractUnchanged(orig RowValues) {
	for k, v := range orig {
		nv, ok := (*rv)[k]
		if ok && bytes.Equal(v.Value, nv.Value) {
			delete(*rv, k)
		}
	}
}

func (rv RowValues) receiverInterfaces(keys []string) []interface{} {
	result := make([]interface{}, len(keys))
	for i, k := range keys {
		rv[k] = &RowValue{}
		result[i] = rv[k]
	}
	return result
}

var columnTypeMap = map[string]string{
	"[]byte":  "blob",
	"bool":    "boolean",
	"float64": "double",
	"github.com/logan/creative/datastore.SeqID": "varchar",
	"int64":     "bigint",
	"string":    "varchar",
	"time.Time": "timestamp",
}

var (
	tiBoolean   = &gocql.TypeInfo{Type: gocql.TypeBoolean}
	tiBlob      = &gocql.TypeInfo{Type: gocql.TypeBlob}
	tiDouble    = &gocql.TypeInfo{Type: gocql.TypeDouble}
	tiBigInt    = &gocql.TypeInfo{Type: gocql.TypeBigInt}
	tiVarchar   = &gocql.TypeInfo{Type: gocql.TypeVarchar}
	tiTimestamp = &gocql.TypeInfo{Type: gocql.TypeTimestamp}
)

var typeInfoMap = map[string]*gocql.TypeInfo{
	"boolean":   tiBoolean,
	"blob":      tiBlob,
	"double":    tiDouble,
	"bigint":    tiBigInt,
	"varchar":   tiVarchar,
	"timestamp": tiTimestamp,
}

var column_validators = map[string]string{
	"org.apache.cassandra.db.marshal.BooleanType":   "boolean",
	"org.apache.cassandra.db.marshal.BytesType":     "blob",
	"org.apache.cassandra.db.marshal.DoubleType":    "double",
	"org.apache.cassandra.db.marshal.LongType":      "bigint",
	"org.apache.cassandra.db.marshal.TimestampType": "timestamp",
	"org.apache.cassandra.db.marshal.UTF8Type":      "varchar",
}

// MarshalRow produces a RowValues map from a Persistable struct with a registered Table.
func MarshalRow(row Persistable) (result RowValues, err error) {
	cf := row.GetCF()
	value := reflect.Indirect(reflect.ValueOf(row))
	result = make(RowValues)
	for _, col := range cf.Columns {
		fieldval := value.FieldByName(col.Name)
		if fieldval.IsValid() {
			var marshalled []byte
			if marshalled, err = gocql.Marshal(col.typeInfo, fieldval.Interface()); err != nil {
				return
			}
			result[col.Name] = &RowValue{marshalled, col.typeInfo}
		}
	}
	return
}

// UnmarshalRow fills in a Persistable struct with a registered Table.
func (rv RowValues) UnmarshalRow(row Persistable) error {
	value := reflect.Indirect(reflect.ValueOf(row))
	if value.Type().Kind() != reflect.Struct {
		return ErrInvalidRowType
	}
	for k, v := range rv {
		target := value.FieldByName(k)
		if !target.IsValid() {
			return ErrInvalidRowType
		}
		if err := gocql.Unmarshal(v.TypeInfo, v.Value, target.Addr().Interface()); err != nil {
			return err
		}
	}
	return nil
}
