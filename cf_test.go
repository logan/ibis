package ibis

import "errors"
import "fmt"
import "reflect"
import "testing"
import "time"

import "github.com/gocql/gocql"

import . "github.com/smartystreets/goconvey/convey"

func TestReflectAndCreate(t *testing.T) {
	type table struct {
		Str    string
		Int    int64
		Bool   bool
		Double float64
		Nested struct {
			Time  time.Time
			Blob  []byte
			SeqID SeqID
			UUID  gocql.UUID
		}
	}

	cf := &CF{name: "test"}
	cf.fillFromRowType(reflect.TypeOf(table{}))
	cf.SetPrimaryKey("Str")

	shouldBeColumn := func(actual interface{}, expected ...interface{}) string {
		idx := actual.(int)
		name := expected[0].(string)
		cqlType := expected[1].(string)

		if idx < 0 || idx >= len(cf.columns) {
			return fmt.Sprintf("column must be in range 0-%d", len(cf.columns)-1)
		}
		if cf.columns[idx].Name != name {
			return fmt.Sprintf("expected column named %#v, found %#v instead", name,
				cf.columns[idx].Name)
		}
		if cf.columns[idx].Type != cqlType {
			return fmt.Sprintf("expected column of type %#v, found %#v instead", cqlType,
				cf.columns[idx].Type)
		}
		return ""
	}

	Convey("Passing a non-struct type to fillFromRowType should panic", t, func() {
		So(func() { cf.fillFromRowType(reflect.TypeOf(8)) }, ShouldPanicWith, "row must be struct")
	})

	Convey("Checking reflected schema", t, func() {
		So(0, shouldBeColumn, "Str", "varchar")
		So(1, shouldBeColumn, "Int", "bigint")
		So(2, shouldBeColumn, "Bool", "boolean")
		So(3, shouldBeColumn, "Double", "double")
		So(4, shouldBeColumn, "Time", "timestamp")
		So(5, shouldBeColumn, "Blob", "blob")
		So(6, shouldBeColumn, "SeqID", "varchar")
		So(7, shouldBeColumn, "UUID", "timeuuid")
	})

	Convey("Checking create statement", t, func() {
		So(cf.CreateStatement().String(), ShouldEqual,
			"CREATE TABLE test (Str varchar, Int bigint, Bool boolean,"+
				" Double double, Time timestamp, Blob blob, SeqID varchar, UUID timeuuid,"+
				" PRIMARY KEY (Str))")

		cf.SetPrimaryKey("Double", "Time", "Blob")
		cf.typeID = 8
		So(cf.CreateStatement().String(), ShouldEqual,
			"CREATE TABLE test (Double double, Time timestamp, Blob blob,"+
				" Str varchar, Int bigint, Bool boolean, SeqID varchar, UUID timeuuid,"+
				" PRIMARY KEY (Double, Time, Blob)) WITH comment='8'")
	})
}

func TestCrud(t *testing.T) {
	type crudRow struct {
		Partition string `ibis:"key"`
		Cluster   int64  `ibis:"key"`
		Value     string
	}
	model := &struct{ Test *CF }{ReflectCF(crudRow{})}
	schema := ReflectTestSchema(t, model)
	defer schema.Cluster.Close()

	cf := model.Test

	Convey("CommitCAS should work exactly once for a given key (\"P1\", 0)", t, func() {
		crud := crudRow{Partition: "P1", Cluster: 0, Value: "P1-0"}
		So(cf.CommitCAS(&crud), ShouldBeNil)
		So(cf.CommitCAS(&crud), ShouldEqual, ErrAlreadyExists)
	})

	Convey("LoadByKey should retrieve (\"P1\", 0)", t, func() {
		var crud crudRow
		So(cf.LoadByKey(&crud, "P1", 0), ShouldBeNil)
		So(crud.Partition, ShouldEqual, "P1")
		So(crud.Cluster, ShouldEqual, 0)
		So(crud.Value, ShouldEqual, "P1-0")
	})

	Convey("LoadByKey should return ErrNotFound for (\"P1\", 1)", t, func() {
		var crud crudRow
		So(cf.LoadByKey(&crud, "P1", 1), ShouldEqual, ErrNotFound)
	})

	Convey("LoadByKey should retrieve (\"P1\", 1) once it's committed", t, func() {
		crud := crudRow{Partition: "P1", Cluster: 1, Value: "P1-1"}
		So(cf.CommitCAS(&crud), ShouldBeNil)
		var retr crudRow
		So(cf.LoadByKey(&retr, "P1", 1), ShouldBeNil)
		So(retr.Partition, ShouldEqual, "P1")
		So(retr.Cluster, ShouldEqual, 1)
		So(retr.Value, ShouldEqual, "P1-1")
	})

	Convey("Commit should update an existing row", t, func() {
		crud := crudRow{Partition: "P1", Cluster: 1, Value: "P1-1 modified"}
		So(cf.CommitCAS(&crud), ShouldEqual, ErrAlreadyExists)
		So(cf.Commit(&crud), ShouldBeNil)
		var retr crudRow
		So(cf.LoadByKey(&retr, "P1", 1), ShouldBeNil)
		So(retr.Partition, ShouldEqual, "P1")
		So(retr.Cluster, ShouldEqual, 1)
		So(retr.Value, ShouldEqual, "P1-1 modified")
	})

	Convey("Checking Exists", t, func() {
		shouldExist := func(actual interface{}, expected ...interface{}) string {
			exp := true
			if len(expected) > 0 {
				exp = expected[0].(bool)
			}
			key := actual.([]interface{})
			b, err := cf.Exists(key...)
			if err != nil {
				return fmt.Sprint(err)
			}
			if exp {
				return ShouldBeTrue(b)
			} else {
				return ShouldBeFalse(b)
			}
		}
		shouldNotExist := func(actual interface{}, expected ...interface{}) string {
			return shouldExist(actual, false)
		}
		So([]interface{}{"P1", 0}, shouldExist)
		So([]interface{}{"P1", 1}, shouldExist)
		So([]interface{}{"P1", 2}, shouldNotExist)
		So([]interface{}{"P2", 0}, shouldNotExist)
	})
}

func TestProvisioning(t *testing.T) {
	type row struct {
		ID string `ibis:"key"`
	}
	model := &struct{ Test *CF }{ReflectCF(row{})}
	schema := ReflectTestSchema(t, model)
	defer schema.Cluster.Close()
	cf := model.Test

	Convey("Requesting unprovisioned interface should return nil", t, func() {
		var c Cluster
		So(cf.GetProvider(&c), ShouldBeFalse)
		So(c, ShouldBeNil)
	})

	Convey("Providing an interface and then requesting it should return it", t, func() {
		var c Cluster
		cf.Provide(schema.Cluster)
		So(cf.GetProvider(&c), ShouldBeTrue)
		So(c, ShouldEqual, schema.Cluster)
		var q Query
		So(cf.GetProvider(&q), ShouldBeFalse)
	})

	Convey("Passing an invalid pointer to GetProvider should panic", t, func() {
		So(func() { cf.GetProvider(8) }, ShouldPanicWith,
			"destination must be a pointer to an interface")
		So(func() { cf.GetProvider(cf) }, ShouldPanicWith,
			"destination must be a pointer to an interface")
	})
}

func TestPrecommitHooks(t *testing.T) {
	type rowType struct {
		ID string `ibis:"key"`
	}
	model := &struct {
		Src  *CF
		Dest *CF
	}{ReflectCF(rowType{}), ReflectCF(rowType{})}
	schema := ReflectTestSchema(t, model)
	defer schema.Cluster.Close()

	src := model.Src
	dest := model.Dest
	failErr := errors.New("failErr")

	src.Precommit(func(row interface{}, mmap MarshaledMap) ([]CQL, error) {
		id := row.(*rowType).ID
		if id == "fail" {
			return nil, failErr
		}
		cql, err := dest.MakeCommit(&rowType{id + "-mirror"})
		if err != nil {
			return nil, err
		}
		return []CQL{cql}, nil
	})

	Convey("Precommit hook should be able to piggyback commits to another table", t, func() {
		So(src.CommitCAS(&rowType{"test"}), ShouldBeNil)
		b, err := dest.Exists("test-mirror")
		So(err, ShouldBeNil)
		So(b, ShouldBeTrue)
	})

	Convey("Precommit error should interrupt commit and percolate back to caller", t, func() {
		err := src.Commit(&rowType{"fail"})
		So(err, ShouldNotBeNil)
		wrapped, ok := err.(WrappedError)
		So(ok, ShouldBeTrue)
		wrapped, ok = wrapped.Unwrap().(WrappedError)
		So(ok, ShouldBeTrue)
		So(wrapped.Unwrap(), ShouldEqual, failErr)
	})
}

func TestMiscCFErrors(t *testing.T) {
	type r struct {
		ID string `ibis:"key"`
	}
	cf := ReflectCF(r{})
	unboundCf := ReflectCF(r{})
	model := &struct{ T *CF }{cf}
	schema := ReflectTestSchema(t, model)
	defer schema.Cluster.Close()

	Convey("Operations on unbound CF should return false, ErrTableNotBound", t, func() {
		_, err := unboundCf.Exists()
		So(err, ShouldEqual, ErrTableNotBound)
		So(unboundCf.Commit(nil), ShouldEqual, ErrTableNotBound)
		So(unboundCf.CommitCAS(nil), ShouldEqual, ErrTableNotBound)
	})

	Convey("LoadByKey errors", t, func() {
		Convey("on unbound CF should return ErrTableNotBound", func() {
			So(unboundCf.LoadByKey(nil), ShouldEqual, ErrTableNotBound)
		})
		Convey("on mismatched key length should return ErrInvalidKey", func() {
			So(cf.LoadByKey(nil), ShouldEqual, ErrInvalidKey)
		})
		Convey("on invalid dest should return ErrInvalidRowType", func() {
			So(cf.Commit(&r{"test"}), ShouldBeNil)
			So(cf.LoadByKey(nil, "test"), ShouldEqual, ErrInvalidRowType)
		})
	})

}

func TestCFQuery(t *testing.T) {
	type rowType struct {
		Partition string `ibis:"key"`
		Cluster   string `ibis:"key"`
	}
	model := &struct{ Test *CF }{ReflectCF(rowType{})}
	schema := ReflectTestSchema(t, model)
	defer schema.Cluster.Close()
	cf := model.Test

	Convey("CFQuery should yield all committed rows", t, func() {
		rows := []*rowType{
			&rowType{"P", "l"},
			&rowType{"P", "o"},
			&rowType{"P", "g"},
			&rowType{"P", "a"},
			&rowType{"P", "n"},
		}
		for _, row := range rows {
			So(cf.CommitCAS(row), ShouldBeNil)
		}
		q := Select().From(cf).Where("Partition = ?", "P").OrderBy("Cluster").CQL().Query()
		cfq := cf.Scanner(q)
		expected := []string{"a", "g", "l", "n", "o"}
		clusters := make([]string, 0)
		var row rowType
		for i := 0; cfq.ScanRow(&row); i++ {
			clusters = append(clusters, row.Cluster)
		}
		So(clusters, ShouldResemble, expected)
		So(cfq.Close(), ShouldBeNil)
	})
}
