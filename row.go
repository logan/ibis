package ibis

import "bytes"
import "errors"
import "fmt"
import "reflect"
import "time"

import "tux21b.org/v1/gocql"

var (
	ErrInvalidRowType = errors.New("row doesn't match schema")
)

var columnTypeMap = map[string]string{
	"[]byte":                      "blob",
	"bool":                        "boolean",
	"float64":                     "double",
	"github.com/logan/ibis.SeqID": "varchar",
	"int64":     "bigint",
	"string":    "varchar",
	"time.Time": "timestamp",
}

var (
	// gocql.TypeInfos that ibis supports.
	TIBoolean   = &gocql.TypeInfo{Type: gocql.TypeBoolean}
	TIBlob      = &gocql.TypeInfo{Type: gocql.TypeBlob}
	TIDouble    = &gocql.TypeInfo{Type: gocql.TypeDouble}
	TIBigInt    = &gocql.TypeInfo{Type: gocql.TypeBigInt}
	TIVarchar   = &gocql.TypeInfo{Type: gocql.TypeVarchar}
	TITimestamp = &gocql.TypeInfo{Type: gocql.TypeTimestamp}
)

var typeInfoMap = map[string]*gocql.TypeInfo{
	"boolean":   TIBoolean,
	"blob":      TIBlob,
	"double":    TIDouble,
	"bigint":    TIBigInt,
	"varchar":   TIVarchar,
	"timestamp": TITimestamp,
}

var column_validators = map[string]string{
	"org.apache.cassandra.db.marshal.BooleanType":   "boolean",
	"org.apache.cassandra.db.marshal.BytesType":     "blob",
	"org.apache.cassandra.db.marshal.DoubleType":    "double",
	"org.apache.cassandra.db.marshal.LongType":      "bigint",
	"org.apache.cassandra.db.marshal.TimestampType": "timestamp",
	"org.apache.cassandra.db.marshal.UTF8Type":      "varchar",
}

// MarshalledValue contains the bytes and type info for a value that has already been marshalled for
// Cassandra.
type MarshalledValue struct {
	Bytes    []byte
	TypeInfo *gocql.TypeInfo
	Dirty    bool
}

// MarshalCQL trivially implements the gocql.Marshaler interface.
func (rv *MarshalledValue) MarshalCQL(info *gocql.TypeInfo) ([]byte, error) {
	return rv.Bytes, nil
}

// UnmarshalCQL trivially implements the gocql.Marshaler interface.
func (rv *MarshalledValue) UnmarshalCQL(info *gocql.TypeInfo, bytes []byte) error {
	rv.Bytes = bytes
	rv.TypeInfo = info
	return nil
}

// MarshalledMap is a map of column names to marshalled values.
type MarshalledMap map[string]*MarshalledValue

// InterfacesFor returns the marshalled values associated with the given keys as bare interfaces.
// They are returned in order corresponding to that of the given keys.
func (rv *MarshalledMap) InterfacesFor(keys ...string) []interface{} {
	result := make([]interface{}, len(keys))
	for i, k := range keys {
		result[i] = (*rv)[k]
	}
	return result
}

// PointersTo associates new marshalled values in the map and returns pointers to them to be filled
// in by methods like Query.Scan. The pointers are returned as a list of interfaces in order
// corresponding to that of the given keys.
func (rv *MarshalledMap) PointersTo(keys ...string) []interface{} {
	result := make([]interface{}, len(keys))
	for i, k := range keys {
		(*rv)[k] = &MarshalledValue{}
		result[i] = (*rv)[k]
	}
	return result
}

// ValuesOf returns the marshalled values associated with the given keys, in the order given by
// keys. Keys with no association will have a corresponding nil value returned.
func (rv *MarshalledMap) ValuesOf(keys ...string) []*MarshalledValue {
	result := make([]*MarshalledValue, len(keys))
	for i, k := range keys {
		result[i] = (*rv)[k]
	}
	return result
}

// Keys returns the keys in the map that have an associated marshalled value, in no particular
// order.
func (rv *MarshalledMap) Keys() []string {
	keys := make([]string, 0, len(*rv))
	for k, v := range *rv {
		if v != nil {
			keys = append(keys, k)
		}
	}
	return keys
}

// DirtyKeys returns the keys in the map that are associated with a dirty marshalled value.
func (rv *MarshalledMap) DirtyKeys() []string {
	dirties := make([]string, 0, len(*rv))
	for k, v := range *rv {
		if v != nil && v.Dirty {
			dirties = append(dirties, k)
		}
	}
	return dirties
}

func (v *MarshalledValue) cmp(w *MarshalledValue) (int, error) {
	if v == nil {
		if w == nil {
			return 0, nil
		} else {
			return -1, nil
		}
	} else if w == nil {
		return 1, nil
	}
	if v.TypeInfo != w.TypeInfo {
		return 0, errors.New("different types are not comparable")
	}

	x, err := unmarshal((*MarshalledValue)(v))
	if err != nil {
		return 0, err
	}
	y, err := unmarshal((*MarshalledValue)(w))
	if err != nil {
		return 0, err
	}

	switch x.(type) {
	case bool:
		b1 := x.(bool)
		b2 := y.(bool)
		if b1 {
			if b2 {
				return 0, nil
			} else {
				return 1, nil
			}
		} else {
			if b2 {
				return -1, nil
			} else {
				return 0, nil
			}
		}
	case []byte:
		b1 := x.([]byte)
		return bytes.Compare(b1, y.([]byte)), nil
	case float64:
		f1 := x.(float64)
		f2 := y.(float64)
		if f1 == f2 {
			return 0, nil
		}
		if f1 < f2 {
			return -1, nil
		}
		return 1, nil
	case int64:
		i1 := x.(int64)
		i2 := y.(int64)
		if i1 == i2 {
			return 0, nil
		}
		if i1 < i2 {
			return -1, nil
		}
		return 1, nil
	case string:
		s1 := x.(string)
		return bytes.Compare([]byte(s1), []byte(y.(string))), nil
	case time.Time:
		t1 := x.(time.Time)
		t2 := y.(time.Time)
		if t1.Equal(t2) {
			return 0, nil
		}
		if t1.Before(t2) {
			return -1, nil
		}
		return 1, nil
	default:
		return 0, errors.New(fmt.Sprintf("don't know how to compare %T", x))
	}
}

// A Row is capable of pointing to its column family and marshalling/unmarshalling itself.
type Row interface {
	CF() *ColumnFamily
	Marshal(MarshalledMap) error
	Unmarshal(MarshalledMap) error
}

type rowReflector struct {
	cf      *ColumnFamily
	rowType reflect.Type
}

func newRowReflector(cf *ColumnFamily, template interface{}) *rowReflector {
	return &rowReflector{cf: cf, rowType: reflect.PtrTo(reflect.TypeOf(template))}
}

func (s *rowReflector) reflectedRow(x interface{}) (Row, error) {
	xValue := reflect.ValueOf(x)
	if !xValue.Type().ConvertibleTo(s.rowType) {
		return nil, ErrInvalidType
	}
	return &reflectedRow{cf: s.cf, value: xValue.Convert(s.rowType).Elem()}, nil
}

type reflectedRow struct {
	cf    *ColumnFamily
	value reflect.Value
}

func (rr *reflectedRow) CF() *ColumnFamily {
	return rr.cf
}

func (rr *reflectedRow) Marshal(mmap MarshalledMap) error {
	var (
		marshalled []byte
		err        error
	)
	for _, col := range rr.cf.Columns {
		fieldval := rr.value.FieldByName(col.Name)
		if fieldval.IsValid() {
			if seqid, ok := fieldval.Interface().(SeqID); ok && seqid == "" {
				if rr.cf.SeqIDGenerator != nil {
					if seqid, err = rr.cf.NewSeqID(); err != nil {
						return err
					}
					fieldval.Set(reflect.ValueOf(seqid))
				}
			}
			if t, ok := fieldval.Interface().(time.Time); ok && t.IsZero() {
				// zero time values aren't marshalled correctly by gocql; they go into cassandra
				// as 1754-08-30 22:43:41.129 +0000 UTC.
				marshalled, err = gocql.Marshal(col.typeInfo, int64(0))
			} else {
				marshalled, err = gocql.Marshal(col.typeInfo, fieldval.Interface())
			}
			if err != nil {
				return err
			}
			mmap[col.Name] = &MarshalledValue{
				Bytes:    marshalled,
				TypeInfo: col.typeInfo,
				Dirty:    true,
			}
		}
	}
	return nil
}

func (rr *reflectedRow) Unmarshal(mmap MarshalledMap) error {
	for k, v := range mmap {
		if v.Bytes != nil {
			target := rr.value.FieldByName(k)
			if !target.IsValid() {
				return ErrInvalidRowType
			}
			if err := gocql.Unmarshal(v.TypeInfo, v.Bytes, target.Addr().Interface()); err != nil {
				return err
			}
			// zero time values aren't unmarshalled correctly by gocql; when cassandra returns a
			// time at 0 relative to its own epoch, we should zero it relative to time.Time's epoch
			if t, ok := target.Addr().Interface().(*time.Time); ok {
				var x int64
				if err := gocql.Unmarshal(TIBigInt, v.Bytes, &x); err != nil {
					return err
				}
				if x == 0 {
					*t = time.Time{}
				}
			}
		}
	}
	return nil
}