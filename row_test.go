package ibis

import "fmt"
import "reflect"
import "testing"
import "time"

func TestMarshalledMapInterfacesForAndPointersTo(t *testing.T) {
	expect := func(interfaces []interface{}, expected ...*MarshalledValue) (string, bool) {
		msg := fmt.Sprintf("\nexpected: %+v\nreceived: %+v", interfaces, expected)
		if len(interfaces) != len(expected) {
			return msg, false
		}
		for i, x := range interfaces {
			msg += fmt.Sprintf("\nmismatch at [%d]: %#v != %#v", i, x, expected[i])
			if x != expected[i] {
				return msg, false
			}
		}
		return "", true
	}

	mmap := make(MarshalledMap)
	if msg, ok := expect(mmap.InterfacesFor()); !ok {
		t.Error(msg)
	}
	if msg, ok := expect(mmap.PointersTo()); !ok {
		t.Error(msg)
	}
	if msg, ok := expect(mmap.InterfacesFor("A", "B", "C"), nil, nil, nil); !ok {
		t.Error(msg)
	}

	ptrs := mmap.PointersTo("A", "B", "C")
	if msg, ok := expect(ptrs, mmap["A"], mmap["B"], mmap["C"]); !ok {
		t.Error(msg)
	}

	var a, b, c MarshalledValue
	mmap["A"] = &a
	mmap["B"] = &b
	mmap["C"] = &c
	if msg, ok := expect(mmap.InterfacesFor("A", "B", "C"), &a, &b, &c); !ok {
		t.Error(msg)
	}

	mmap.PointersTo("A", "B", "C")
	if mmap["A"] == &a || mmap["B"] == &b || mmap["C"] == &c {
		t.Error("mmap.PointersTo(...) should have reset the values under the given keys")
	}
}

func TestMarshalledMapDirtyKeys(t *testing.T) {
	mmap := make(MarshalledMap)

	expect := func(expected ...string) (string, bool) {
		received := mmap.DirtyKeys()
		msg := fmt.Sprintf("\nexpected: %+v\nreceived: %+v", received, expected)
		if len(received) == 0 {
			return msg, len(expected) == 0
		}
		return msg, reflect.DeepEqual(received, expected)
	}

	if msg, ok := expect(); !ok {
		t.Error(msg)
	}
	mmap["A"] = &MarshalledValue{Dirty: true}
	mmap["B"] = nil
	mmap["C"] = &MarshalledValue{Dirty: false}
	mmap["D"] = &MarshalledValue{Dirty: true}
	if msg, ok := expect("A", "D"); !ok {
		t.Error(msg)
	}
}

type marshalTestRow struct {
	Str   string
	Int   int64
	Time  time.Time
	SeqID SeqID
}

type marshalTestTable struct {
	*ColumnFamily
}

func (t *marshalTestTable) CF() *ColumnFamily {
	t.ColumnFamily = ReflectColumnFamily(marshalTestRow{})
	return t.ColumnFamily
}

func TestReflectedMarshalAndUnmarshal(t *testing.T) {
	model := struct{ T *marshalTestTable }{}
	ReflectSchema(&model)
	cf := model.T.ColumnFamily
	g := FakeSeqIDGenerator(36 * 36 * 36) // "1000" in base-36
	cf.SeqIDGenerator = &g

	var r marshalTestRow

	check := func() (string, bool) {
		mmap, err := cf.marshal(&r)
		if err != nil {
			return fmt.Sprintf("unexpected error: %v", err), false
		}
		var s marshalTestRow
		if err = cf.unmarshal(&s, mmap); err != nil {
			return fmt.Sprintf("unexpected error: %v", err), false
		}
		if r.Str != s.Str || r.Int != s.Int || r.Time != s.Time || r.SeqID != s.SeqID {
			return fmt.Sprintf("\nexpected: %+v\nreceived: %+v", r, s), false
		}
		return "", true
	}

	check()
	if r.SeqID != "1001" {
		t.Errorf("expected seqid %s, got %s", "1001", r.SeqID)
	}

	r.Str = "str"
	r.Int = 8
	r.Time = time.Now()
	r.SeqID = ""
	check()
	if r.SeqID != "1002" {
		t.Errorf("expected seqid %s, got %s", "1002", r.SeqID)
	}
}
