package datastore

import "fmt"
import "reflect"
import "testing"
import "time"

func TestFillFromRowTypeAndKeyAndCreateStatement(t *testing.T) {
	type table struct {
		Str    string
		Int    int64
		Bool   bool
		Double float64
		Time   time.Time
		Blob   []byte
		SeqID  SeqID
	}

	cf := &ColumnFamily{}
	cf.fillFromRowType("test", reflect.TypeOf(&table{}))
	cf.Key("Str")

	expect := func(expected string) (string, bool) {
		received := cf.CreateStatement().String()
		if expected != received {
			return fmt.Sprintf("\nexpected: %s\nreceived: %s", expected, received), false
		}
		return "", true
	}

	if msg, ok := expect("CREATE TABLE test (Str varchar, Int bigint, Bool boolean," +
		" Double double, Time timestamp, Blob blob, SeqID varchar," +
		" PRIMARY KEY (Str))"); !ok {
		t.Error(msg)
	}

	cf.Key("Double", "Time", "Blob")
	cf.typeID = 8

	if msg, ok := expect("CREATE TABLE test (Double double, Time timestamp, Blob blob," +
		" Str varchar, Int bigint, Bool boolean, SeqID varchar," +
		" PRIMARY KEY (Double, Time, Blob)) WITH comment='8'"); !ok {
		t.Error(msg)
	}
}