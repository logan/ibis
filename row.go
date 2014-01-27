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
	"[]byte":  "blob",
	"bool":    "boolean",
	"float64": "double",
	"github.com/logan/ibis.SeqID": "varchar",
	"int64":     "bigint",
	"string":    "varchar",
	"time.Time": "timestamp",
}

var (
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

type MarshalledValue struct {
	Bytes    []byte
	TypeInfo *gocql.TypeInfo
	Dirty    bool
}

func (rv *MarshalledValue) MarshalCQL(info *gocql.TypeInfo) ([]byte, error) {
	return rv.Bytes, nil
}

func (rv *MarshalledValue) UnmarshalCQL(info *gocql.TypeInfo, bytes []byte) error {
	rv.Bytes = bytes
	rv.TypeInfo = info
	return nil
}

type MarshalledMap map[string]*MarshalledValue

func (rv *MarshalledMap) InterfacesFor(keys ...string) []interface{} {
	result := make([]interface{}, len(keys))
	for i, k := range keys {
		result[i] = (*rv)[k]
	}
	return result
}

func (rv *MarshalledMap) PointersTo(keys ...string) []interface{} {
	result := make([]interface{}, len(keys))
	for i, k := range keys {
		(*rv)[k] = &MarshalledValue{}
		result[i] = (*rv)[k]
	}
	return result
}

func (rv *MarshalledMap) ValuesOf(keys ...string) []*MarshalledValue {
	result := make([]*MarshalledValue, len(keys))
	for i, k := range keys {
		result[i] = (*rv)[k]
	}
	return result
}

func (rv *MarshalledMap) Keys() []string {
	keys := make([]string, 0, len(*rv))
	for k, v := range *rv {
		if v != nil {
			keys = append(keys, k)
		}
	}
	return keys
}

func (rv *MarshalledMap) DirtyKeys() []string {
	dirties := make([]string, 0, len(*rv))
	for k, v := range *rv {
		if v != nil && v.Dirty {
			dirties = append(dirties, k)
		}
	}
	return dirties
}

func (v *MarshalledValue) Cmp(w *MarshalledValue) (int, error) {
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
	GetCF() *ColumnFamily
	Marshal(MarshalledMap) error
	Unmarshal(MarshalledMap) error
}

type ReflectedRow struct {
	CF     *ColumnFamily `json:"-"`
	self   Row
	loaded MarshalledMap
	total  MarshalledMap
	dirty  MarshalledMap
}

func (s *ReflectedRow) Reflect(self Row) Row {
	s.self = self
	return s.self
}

func (s *ReflectedRow) GetCF() *ColumnFamily {
	return s.CF
}

func (s *ReflectedRow) loadedMap() MarshalledMap {
	if s.loaded == nil {
		s.loaded = make(MarshalledMap)
	}
	return s.loaded
}

func (s *ReflectedRow) Marshal(mmap MarshalledMap) error {
	var (
		marshalled []byte
		err        error
	)
	cf := s.GetCF()
	loaded := s.loadedMap()
	value := reflect.Indirect(reflect.ValueOf(s.self))
	for _, col := range cf.Columns {
		fieldval := value.FieldByName(col.Name)
		if fieldval.IsValid() {
			if seqid, ok := fieldval.Interface().(SeqID); ok && seqid == "" {
				if cf.SeqIDGenerator != nil {
					if seqid, err = cf.NewSeqID(); err != nil {
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
			mmap[col.Name] = &MarshalledValue{Bytes: marshalled, TypeInfo: col.typeInfo}
			if prev, ok := loaded[col.Name]; !ok || !bytes.Equal(prev.Bytes, marshalled) {
				mmap[col.Name].Dirty = true
			}
		}
	}
	return nil
}

func (s *ReflectedRow) Unmarshal(mmap MarshalledMap) error {
	s.loaded = make(MarshalledMap)
	value := reflect.Indirect(reflect.ValueOf(s.self))
	for k, v := range mmap {
		if v.Bytes != nil {
			target := value.FieldByName(k)
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
			s.loaded[k] = v
		}
	}
	return nil
}
