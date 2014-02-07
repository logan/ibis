package ibis

import "encoding/json"
import "fmt"
import "testing"
import "time"

import "github.com/gocql/gocql"
import "github.com/smartystreets/goconvey/reporting"
import . "github.com/smartystreets/goconvey/convey"

func shouldEqualMarshaledValue(actual interface{}, expected ...interface{}) string {
	left := actual.(*MarshaledValue)
	right := expected[0].(*MarshaledValue)
	cmp, err := left.cmp(right)
	if err != nil {
		return ShouldNotBeNil(err)
	}
	if cmp == 0 {
		return ""
	}
	report := reporting.FailureView{
		Message:  "marshaled values did not match",
		Expected: fmt.Sprintf("%+v", right),
		Actual:   fmt.Sprintf("%+v", left),
	}
	if bs, err := json.Marshal(report); err == nil {
		return string(bs)
	}
	return "marshaled values did not match"
}

func TestMarshaledValue(t *testing.T) {
	Convey("Dirty states", t, func() {
		v := MarshaledValue{}
		So(v.Dirty(), ShouldBeFalse)
		v.MarkDirty() // should fail because v has no marshaled bytes
		So(v.Dirty(), ShouldBeFalse)

		v.Bytes = []byte{0}
		So(v.Dirty(), ShouldBeTrue)
		v.OriginalBytes = []byte{0}
		So(v.Dirty(), ShouldBeFalse)
		v.Bytes[0] = 1
		So(v.Dirty(), ShouldBeTrue)

		v.MarkClean()
		So(v.Dirty(), ShouldBeFalse)
		So(v.OriginalBytes, ShouldResemble, []byte{1})
		So(v.String(), ShouldNotEndWith, " (dirty)")

		v.MarkDirty()
		So(v.Dirty(), ShouldBeTrue)
		So(v.OriginalBytes, ShouldBeNil)
		So(v.String(), ShouldEndWith, " (dirty)")
	})

	Convey("Comparisons", t, func() {
		compare := func(v1, v2 *MarshaledValue, expectInt int, expectError bool) string {
			c, err := v1.cmp(v2)
			if expectError {
				return ShouldNotBeNil(err)
			}
			if msg := ShouldBeNil(err); msg != "" {
				return msg
			}
			return ShouldEqual(c, expectInt)
		}

		shouldFailToCompare := func(actual interface{}, expected ...interface{}) string {
			return compare(actual.(*MarshaledValue), expected[0].(*MarshaledValue), 0, true)
		}

		shouldEq := func(actual interface{}, expected ...interface{}) string {
			return compare(actual.(*MarshaledValue), expected[0].(*MarshaledValue), 0, false)
		}

		shouldLt := func(actual interface{}, expected ...interface{}) string {
			return compare(actual.(*MarshaledValue), expected[0].(*MarshaledValue), -1, false)
		}

		shouldGt := func(actual interface{}, expected ...interface{}) string {
			return compare(actual.(*MarshaledValue), expected[0].(*MarshaledValue), 1, false)
		}

		Convey("nil and error scenarios", func() {
			var v1, v2 *MarshaledValue
			So(v1, shouldEq, v2)
			v1 = LiteralValue(1)
			So(v1, shouldGt, v2)
			So(v2, shouldLt, v1)

			// Comparing different types should fail.
			v2 = &MarshaledValue{TypeInfo: TIUUID, Bytes: []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0}}
			So(v1, shouldFailToCompare, v2)

			// Force a marshal error in v1.
			v1 = &MarshaledValue{TypeInfo: TIUUID, Bytes: []byte{0}}
			So(v1, shouldFailToCompare, v2)
			So(v2, shouldFailToCompare, v1)
		})

		Convey("bools", func() {
			So(LiteralValue(true), shouldEq, LiteralValue(true))
			So(LiteralValue(false), shouldEq, LiteralValue(false))
			So(LiteralValue(true), shouldGt, LiteralValue(false))
			So(LiteralValue(false), shouldLt, LiteralValue(true))
		})

		Convey("blobs", func() {
			v1 := LiteralValue([]byte{0})
			v2 := LiteralValue([]byte{0})
			So(v1, shouldEq, v2)

			v1.Bytes = []byte{0, 0}
			So(v1, shouldGt, v2)
			So(v2, shouldLt, v1)
		})

		Convey("doubles", func() {
			v1 := LiteralValue(3.14)
			v2 := LiteralValue(3.14)
			So(v1, shouldEq, v2)

			v1 = LiteralValue(3.142)
			So(v1, shouldGt, v2)
			So(v2, shouldLt, v1)
		})

		Convey("longs", func() {
			v1 := LiteralValue(8)
			v2 := LiteralValue(8)
			So(v1, shouldEq, v2)

			v1 = LiteralValue(9)
			So(v1, shouldGt, v2)
			So(v2, shouldLt, v1)
		})

		Convey("strings", func() {
			v1 := LiteralValue("test")
			v2 := LiteralValue("test")
			So(v1, shouldEq, v2)

			v1 = LiteralValue("testing")
			So(v1, shouldGt, v2)
			So(v2, shouldLt, v1)
		})

		Convey("timestamps", func() {
			now := time.Now()
			v1 := LiteralValue(now)
			v2 := LiteralValue(now)
			So(v1, shouldEq, v2)

			v1 = LiteralValue(now.Add(time.Second))
			So(v1, shouldGt, v2)
			So(v2, shouldLt, v1)
		})

		Convey("uuids", func() {
			now := time.Now()
			v1 := LiteralValue(UUIDFromTime(now))
			v2 := LiteralValue(UUIDFromTime(now))
			So(v1, shouldEq, v2)

			v1 = LiteralValue(UUIDFromTime(now.Add(time.Second)))
			So(v1, shouldGt, v2)
			So(v2, shouldLt, v1)
		})
	})
}

func TestMarshaledMap(t *testing.T) {
	Convey("Nil/empty map", t, func() {
		var mmap MarshaledMap
		var nilV *MarshaledValue

		So(len(mmap.InterfacesFor()), ShouldEqual, 0)
		So(len(mmap.PointersTo()), ShouldEqual, 0)
		So(mmap.InterfacesFor("A", "B", "C"), ShouldResemble, []interface{}{nilV, nilV, nilV})

		mmap = make(MarshaledMap)
		So(len(mmap.InterfacesFor()), ShouldEqual, 0)
		So(len(mmap.PointersTo()), ShouldEqual, 0)
		So(mmap.InterfacesFor("A", "B", "C"), ShouldResemble, []interface{}{nilV, nilV, nilV})

		ptrs := mmap.PointersTo("A", "B", "C")
		So(len(ptrs), ShouldEqual, 3)
		So(ptrs[0], ShouldEqual, mmap["A"])
		So(ptrs[1], ShouldEqual, mmap["B"])
		So(ptrs[2], ShouldEqual, mmap["C"])
	})

	Convey("Populated map", t, func() {
		now := time.Now()
		mmap := MarshaledMap{
			"str":    LiteralValue("str"),
			"long":   LiteralValue(1),
			"double": LiteralValue(3.14),
			"bool":   LiteralValue(true),
			"blob":   LiteralValue([]byte{1, 2, 3}),
			"time":   LiteralValue(now),
			"uuid":   LiteralValue(UUIDFromTime(now)),
			"nil":    nil,
		}

		keys := mmap.Keys()
		So(keys, ShouldResemble, []string{"str", "long", "double", "bool", "blob", "time", "uuid"})

		interfaces := mmap.InterfacesFor(keys...)
		So(len(interfaces), ShouldEqual, 7)
		So(interfaces[0], shouldEqualMarshaledValue, LiteralValue("str"))
		So(interfaces[1], shouldEqualMarshaledValue, LiteralValue(1))
		So(interfaces[2], shouldEqualMarshaledValue, LiteralValue(3.14))
		So(interfaces[3], shouldEqualMarshaledValue, LiteralValue(true))
		So(interfaces[4], shouldEqualMarshaledValue, LiteralValue([]byte{1, 2, 3}))
		So(interfaces[5], shouldEqualMarshaledValue, LiteralValue(now))
		So(interfaces[6], shouldEqualMarshaledValue, LiteralValue(UUIDFromTime(now)))

		interfaces = mmap.InterfacesFor("nil")
		So(len(interfaces), ShouldEqual, 1)
		So(interfaces[0], shouldEqualMarshaledValue, (*MarshaledValue)(nil))
	})

	Convey("PointersTo should replace existing values", t, func() {
		mmap := MarshaledMap{"x": LiteralValue("x")}
		So(mmap["x"], shouldEqualMarshaledValue, LiteralValue("x"))
		So(mmap["y"], ShouldBeNil)
		ptrs := mmap.PointersTo("x", "y")
		So(len(ptrs), ShouldEqual, 2)
		So(*(ptrs[0].(*MarshaledValue)), ShouldResemble, MarshaledValue{})
		So(*(ptrs[1].(*MarshaledValue)), ShouldResemble, MarshaledValue{})
		ptrs[0] = *LiteralValue("new x")
		ptrs[1] = *LiteralValue("new y")
		So(mmap["x"], shouldEqualMarshaledValue, LiteralValue("new x"))
		So(mmap["y"], shouldEqualMarshaledValue, LiteralValue("new y"))
	})

	Convey("DirtyKeys should return the keys of dirty values", t, func() {
		mmap := MarshaledMap{"x": LiteralValue("x"), "y": LiteralValue("y")}
		So(mmap.DirtyKeys(), ShouldResemble, []string{"x", "y"})

		mmap["x"].MarkClean()
		mmap["y"].MarkClean()
		So(len(mmap.DirtyKeys()), ShouldEqual, 0)
	})
}

type marshalTestRow struct {
	Str   string
	Int   int64
	Time  time.Time
	SeqID SeqID
}

type marshalTestTable struct {
	*CF
}

func (t *marshalTestTable) NewCF() *CF {
	t.CF = ReflectCF(marshalTestRow{})
	return t.CF
}

func TestRowReflection(t *testing.T) {
	type item struct {
		X          string
		Y          int64
		Z          time.Time
		unexported string
	}
	cf := NewCF("",
		Column{Name: "X", Type: "varchar", typeInfo: TIVarchar},
		Column{Name: "Y", Type: "bigint", typeInfo: TIBigInt},
		Column{Name: "Z", Type: "timestamp", typeInfo: TITimestamp})

	Convey("Invalid row types should cause errors to be returned", t, func() {
		reflector := newRowReflector(cf, 1)
		_, err := reflector.reflectedRow("1")
		So(err, ShouldEqual, ErrInvalidRowType)
		_, err = reflector.reflectedRow(nil)
		So(err, ShouldEqual, ErrInvalidRowType)

		reflector = newRowReflector(cf, item{})
		_, err = reflector.reflectedRow("1")
		So(err, ShouldEqual, ErrInvalidRowType)
		_, err = reflector.reflectedRow(nil)
		So(err, ShouldEqual, ErrInvalidRowType)

		_, err = reflector.reflectedRow(item{})
		So(err, ShouldEqual, ErrInvalidRowType)

		_, err = reflector.reflectedRow((*item)(nil))
		So(err, ShouldEqual, ErrInvalidRowType)
	})

	Convey("Magic adaptation to Row interface should occur", t, func() {
		var (
			row Row
			err error
			tgt item
		)

		reflector := newRowReflector(cf, item{})
		row, err = reflector.reflectedRow(&tgt)
		So(err, ShouldBeNil)

		zeroMap := MarshaledMap{}
		So(row.Marshal(zeroMap), ShouldBeNil)

		tgt.X = "x"
		tgt.Y = 8
		tgt.Z = time.Now()
		mmap := MarshaledMap{}
		So(row.Marshal(mmap), ShouldBeNil)
		So(mmap, ShouldResemble,
			MarshaledMap{"X": LiteralValue("x"), "Y": LiteralValue(8), "Z": LiteralValue(tgt.Z)})

		So(row.Unmarshal(zeroMap), ShouldBeNil)
		So(tgt, ShouldResemble, item{})

	})

	Convey("SeqID should get auto-filled", t, func() {
		var s struct{ S SeqID }
		var row Row
		var err error
		cf := NewCF("", Column{Name: "S", Type: "varchar", typeInfo: TIVarchar})

		Convey("No autofill if no generator", func() {
			reflector := newRowReflector(cf, s)
			row, err = reflector.reflectedRow(&s)
			So(err, ShouldBeNil)

			mmap := MarshaledMap{}
			So(row.Marshal(mmap), ShouldBeNil)
			So(row.Unmarshal(mmap), ShouldBeNil)
			So(s.S, ShouldEqual, "")
		})

		Convey("Generator should populate SeqID field", func() {
			seqids := new(FakeSeqIDGenerator).Set(36 * 36 * 36) // next seqid should be "1000"
			schema := NewSchema()
			schema.Provide(SeqIDGenerator(seqids))
			schema.AddCF(cf)

			reflector := newRowReflector(cf, s)
			row, err = reflector.reflectedRow(&s)
			So(err, ShouldBeNil)

			mmap := MarshaledMap{}
			So(row.Marshal(mmap), ShouldBeNil)
			So(row.Unmarshal(mmap), ShouldBeNil)
			So(s.S, ShouldEqual, "1000")
		})
	})
}

func TestTimeUUID(t *testing.T) {
	Convey("IsSet method on TimeUUID", t, func() {
		var t TimeUUID
		So(t.IsSet(), ShouldBeFalse)

		now := time.Now()
		t = UUIDFromTime(now)
		So(t.IsSet(), ShouldBeTrue)

		t.Unset()
		So(t.IsSet(), ShouldBeFalse)
	})

	Convey("Marshaling should work", t, func() {
		var t TimeUUID
		now := time.Now()

		b, err := t.MarshalCQL(TIVarchar)
		So(err, ShouldNotBeNil)

		b, err = t.MarshalCQL(TIBlob)
		So(err, ShouldBeNil)
		So(len(b), ShouldEqual, 0)

		b, err = t.MarshalCQL(TIUUID)
		So(err, ShouldBeNil)
		So(len(b), ShouldEqual, 0)

		t = UUIDFromTime(now)
		bs := gocql.UUID(t).Bytes()

		b, err = t.MarshalCQL(TIUUID)
		So(err, ShouldBeNil)
		So(b, ShouldResemble, bs)
	})

	Convey("Unmarshaling should work", t, func() {
		now := time.Now()
		t := UUIDFromTime(now)
		So(t.IsSet(), ShouldBeTrue)

		So(t.UnmarshalCQL(TIVarchar, []byte{}), ShouldNotBeNil)

		So(t.UnmarshalCQL(TIUUID, []byte{}), ShouldBeNil)
		So(t.IsSet(), ShouldBeFalse)

		So(t.UnmarshalCQL(TIBlob, []byte{}), ShouldBeNil)
		So(t.IsSet(), ShouldBeFalse)

		bs := gocql.UUIDFromTime(now).Bytes()
		So(t.UnmarshalCQL(TIUUID, bs), ShouldBeNil)
		So((gocql.UUID(t)).Bytes(), ShouldResemble, bs)

		So(t.UnmarshalCQL(TIUUID, []byte{0}), ShouldNotBeNil)
	})

	Convey("String should work", t, func() {
		u, _ := gocql.UUIDFromBytes([]byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})
		t := TimeUUID(u)
		So(t.String(), ShouldEqual, "00000000-0000-0000-0000-000000000000")
	})
}
