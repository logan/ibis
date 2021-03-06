package ibis

import "bytes"
import "errors"
import "fmt"
import "reflect"
import "time"

import "github.com/gocql/gocql"

type TimeUUID gocql.UUID

func UUIDFromTime(t time.Time) TimeUUID {
	return TimeUUID(gocql.UUIDFromTime(t))
}

func (id TimeUUID) String() string {
	return gocql.UUID(id).String()
}

func (id TimeUUID) IsSet() bool {
	var zero gocql.UUID
	return !bytes.Equal(gocql.UUID(id).Bytes(), zero.Bytes())
}

func (id *TimeUUID) Unset() {
	var zero TimeUUID
	*id = zero
}

func (id TimeUUID) MarshalCQL(info *gocql.TypeInfo) ([]byte, error) {
	switch info.Type {
	case gocql.TypeBlob, gocql.TypeUUID, gocql.TypeTimeUUID:
		if !id.IsSet() {
			return []byte{}, nil
		}
		return gocql.UUID(id).Bytes(), nil
	default:
		return nil, errors.New(fmt.Sprintf("ibis can't marshal %T into %s", id, info))
	}
}

func (id *TimeUUID) UnmarshalCQL(info *gocql.TypeInfo, data []byte) error {
	switch info.Type {
	case gocql.TypeBlob, gocql.TypeUUID, gocql.TypeTimeUUID:
		if len(data) == 0 {
			id.Unset()
			return nil
		}
		uuid, err := gocql.UUIDFromBytes(data)
		if err != nil {
			return err
		}
		*id = TimeUUID(uuid)
		return nil
	default:
		return errors.New(fmt.Sprintf("ibis can't unmarshal %T from %s", *id, info))
	}
}

var columnTypeMap = map[string]string{
	"[]byte":                         "blob",
	"bool":                           "boolean",
	"float64":                        "double",
	"github.com/logan/ibis.SeqID":    "varchar",
	"github.com/logan/ibis.TimeUUID": "timeuuid",
	"github.com/gocql/gocql.UUID":    "timeuuid",
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
	TIUUID      = &gocql.TypeInfo{Type: gocql.TypeTimeUUID}
)

var typeInfoMap = map[string]*gocql.TypeInfo{
	"boolean":   TIBoolean,
	"blob":      TIBlob,
	"double":    TIDouble,
	"bigint":    TIBigInt,
	"varchar":   TIVarchar,
	"timestamp": TITimestamp,
	"timeuuid":  TIUUID,
}

var column_validators = map[string]string{
	"org.apache.cassandra.db.marshal.BooleanType":   "boolean",
	"org.apache.cassandra.db.marshal.BytesType":     "blob",
	"org.apache.cassandra.db.marshal.DoubleType":    "double",
	"org.apache.cassandra.db.marshal.LongType":      "bigint",
	"org.apache.cassandra.db.marshal.TimestampType": "timestamp",
	"org.apache.cassandra.db.marshal.UTF8Type":      "varchar",
	"org.apache.cassandra.db.marshal.TimeUUIDType":  "timeuuid",
}

// MarshaledValue contains the bytes and type info for a value that has already been marshaled for
// Cassandra.
type MarshaledValue struct {
	Bytes         []byte
	TypeInfo      *gocql.TypeInfo
	OriginalBytes []byte
}

// MarshalCQL trivially implements the gocql.Marshaler interface.
func (rv *MarshaledValue) MarshalCQL(info *gocql.TypeInfo) ([]byte, error) {
	return rv.Bytes, nil
}

// UnmarshalCQL trivially implements the gocql.Marshaler interface.
func (rv *MarshaledValue) UnmarshalCQL(info *gocql.TypeInfo, bytes []byte) error {
	rv.Bytes = bytes
	rv.TypeInfo = info
	return nil
}

// Dirty returns true if a MarshaledValue's Bytes are the same as its OriginalBytes.
func (rv *MarshaledValue) Dirty() bool {
	return !bytes.Equal(rv.Bytes, rv.OriginalBytes)
}

// MarkDirty forces the value to appear dirty.
func (rv *MarshaledValue) MarkDirty() *MarshaledValue {
	rv.OriginalBytes = nil
	return rv
}

// MarkClean forces the value to appear clean.
func (rv *MarshaledValue) MarkClean() *MarshaledValue {
	rv.OriginalBytes = rv.Bytes
	return rv
}

func (rv *MarshaledValue) String() string {
	var dirty string
	if rv.Dirty() {
		dirty = " (dirty)"
	}
	return fmt.Sprintf("%+v%s", rv.Bytes, dirty)
}

func (v *MarshaledValue) cmp(w *MarshaledValue) (int, error) {
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

	x, err := unmarshal((*MarshaledValue)(v))
	if err != nil {
		return 0, err
	}
	y, err := unmarshal((*MarshaledValue)(w))
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
	case gocql.UUID:
		t1 := x.(gocql.UUID).Time()
		t2 := y.(gocql.UUID).Time()
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

// MarshaledMap is a map of column names to marshaled values.
type MarshaledMap map[string]*MarshaledValue

// InterfacesFor returns the marshaled values associated with the given keys as bare interfaces.
// They are returned in order corresponding to that of the given keys.
func (rv *MarshaledMap) InterfacesFor(keys ...string) []interface{} {
	result := make([]interface{}, len(keys))
	for i, k := range keys {
		result[i] = (*rv)[k]
	}
	return result
}

// PointersTo associates new marshaled values in the map and returns pointers to them to be filled
// in by methods like Query.Scan. The pointers are returned as a list of interfaces in order
// corresponding to that of the given keys.
func (rv *MarshaledMap) PointersTo(keys ...string) []interface{} {
	result := make([]interface{}, len(keys))
	for i, k := range keys {
		(*rv)[k] = &MarshaledValue{}
		result[i] = (*rv)[k]
	}
	return result
}

// ValuesOf returns the marshaled values associated with the given keys, in the order given by
// keys. Keys with no association will have a corresponding nil value returned.
func (rv *MarshaledMap) ValuesOf(keys ...string) []*MarshaledValue {
	result := make([]*MarshaledValue, len(keys))
	for i, k := range keys {
		result[i] = (*rv)[k]
	}
	return result
}

// Keys returns the keys in the map that have an associated marshaled value, in no particular
// order.
func (rv *MarshaledMap) Keys() []string {
	keys := make([]string, 0, len(*rv))
	for k, v := range *rv {
		if v != nil {
			keys = append(keys, k)
		}
	}
	return keys
}

// DirtyKeys returns the keys in the map that are associated with a dirty marshaled value.
func (rv *MarshaledMap) DirtyKeys() []string {
	dirties := make([]string, 0, len(*rv))
	for k, v := range *rv {
		if v != nil && v.Dirty() {
			dirties = append(dirties, k)
		}
	}
	return dirties
}

// A Row is capable of pointing to its column family and marshaling/unmarshaling itself.
type Row interface {
	Marshal(MarshaledMap) error
	Unmarshal(MarshaledMap) error
}

type rowReflector struct {
	cf             *CF
	rowType        reflect.Type
	marshalPlugins []reflect.StructField
}

func newRowReflector(cf *CF, template interface{}) *rowReflector {
	return &rowReflector{
		cf:             cf,
		rowType:        reflect.PtrTo(reflect.TypeOf(template)),
		marshalPlugins: make([]reflect.StructField, 0),
	}
}

func (s *rowReflector) addMarshalPlugin(field reflect.StructField) {
	s.marshalPlugins = append(s.marshalPlugins, field)
}

func (s *rowReflector) reflectedRow(x interface{}) (Row, error) {
	xType := reflect.TypeOf(x)
	if xType == nil {
		return nil, ErrInvalidRowType.New()
	}
	xValue := reflect.ValueOf(x)
	if !xType.ConvertibleTo(s.rowType) {
		return nil, ErrInvalidRowType.New()
	}
	if xValue.IsNil() {
		return nil, ErrInvalidRowType.New()
	}
	value := xValue.Convert(s.rowType).Elem()

	marshalPlugins := make([]MarshalPlugin, len(s.marshalPlugins))
	for i, field := range s.marshalPlugins {
		pluginValue := value.FieldByIndex(field.Index)
		if pluginValue.IsNil() {
			pluginValue.Set(reflect.New(field.Type.Elem()))
		}
		marshalPlugins[i] = pluginValue.Interface().(MarshalPlugin)
	}

	rrow := &reflectedRow{
		cf:             s.cf,
		value:          value,
		marshalPlugins: marshalPlugins,
	}
	return rrow, nil
}

type reflectedRow struct {
	cf             *CF
	value          reflect.Value
	marshalPlugins []MarshalPlugin
}

func (rr *reflectedRow) Marshal(mmap MarshaledMap) error {
	var (
		marshaled []byte
		err       error
	)
	for _, col := range rr.cf.columns {
		fieldval := rr.value.FieldByName(col.Name)
		if fieldval.IsValid() {
			if seqid, ok := fieldval.Interface().(SeqID); ok && seqid == "" {
				var gen SeqIDGenerator
				if rr.cf.Schema() != nil && rr.cf.Schema().GetProvider(&gen) {
					if seqid, err = gen.NewSeqID(); err != nil {
						return err
					}
					fieldval.Set(reflect.ValueOf(seqid))
				}
			}
			if t, ok := fieldval.Interface().(time.Time); ok && t.IsZero() {
				// zero time values aren't marshaled correctly by gocql; they go into cassandra
				// as 1754-08-30 22:43:41.129 +0000 UTC.
				marshaled, err = gocql.Marshal(col.typeInfo, int64(0))
			} else {
				marshaled, err = gocql.Marshal(col.typeInfo, fieldval.Interface())
			}
			if err != nil {
				return err
			}
			mmap[col.Name] = &MarshaledValue{
				Bytes:    marshaled,
				TypeInfo: col.typeInfo,
			}
		}
	}
	for _, plugin := range rr.marshalPlugins {
		if err = plugin.OnMarshal(mmap); err != nil {
			return err
		}
	}
	return nil
}

func (rr *reflectedRow) Unmarshal(mmap MarshaledMap) error {
	for k, v := range mmap {
		if v.Bytes != nil {
			target := rr.value.FieldByName(k)
			if !target.IsValid() {
				return ErrInvalidRowType.New()
			}
			if err := gocql.Unmarshal(v.TypeInfo, v.Bytes, target.Addr().Interface()); err != nil {
				return err
			}
			// zero time values aren't unmarshaled correctly by gocql; when cassandra returns a
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
	for _, plugin := range rr.marshalPlugins {
		if err := plugin.OnUnmarshal(mmap); err != nil {
			return err
		}
	}
	return nil
}
