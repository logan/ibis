package ibis

import "testing"

import . "github.com/smartystreets/goconvey/convey"

func TestCQL(t *testing.T) {
	Convey("Bind should produce proper CQL value", t, func() {
		cql := PreparedCQL("stmt").Bind(1, 2, 3)
		So(cql.PreparedCQL, ShouldEqual, "stmt")
		So(cql.params, ShouldResemble, []interface{}{1, 2, 3})
	})

	Convey("String should return just the PreparedCQL", t, func() {
		cql := PreparedCQL("stmt").Bind(1, 2, 3)
		So(cql.String(), ShouldEqual, "stmt")
	})
}

func TestCQLBuilder(t *testing.T) {
	Convey("Empty builder should return empty CQL", t, func() {
		var b CQLBuilder
		cql := b.CQL()
		So(cql.String(), ShouldEqual, "")
		So(len(cql.params), ShouldEqual, 0)
		b.Append("test", 1)
		b.Clear()
		So(cql.String(), ShouldEqual, "")
		So(len(cql.params), ShouldEqual, 0)
	})

	Convey("Constructed builder should join elements properly", t, func() {
		var b CQLBuilder
		b.Append("just text")
		b.Append(" more text", "arg1", "arg2")
		b.Append(" and yet more text", "arg3")
		var b2 CQLBuilder
		b2.Append(" and some cql", "arg4", "arg5")
		b.AppendCQL(b2.CQL())
		cql := b.CQL()
		So(cql.String(), ShouldEqual, "just text more text and yet more text and some cql")
		So(cql.params, ShouldResemble, []interface{}{"arg1", "arg2", "arg3", "arg4", "arg5"})
	})
}

func TestSelectBuilder(t *testing.T) {
	type spec struct {
		X string `ibis:"key"`
		Y string
		Z string
	}
	cf, err := ReflectCF(spec{})
	if err != nil {
		t.Fatal(err)
	}
	model := &struct{ Test *CF }{cf}
	schema := ReflectTestSchema(t, model)
	defer schema.Cluster.Close()

	Convey("SelectBuilder specifies columns correctly", t, func() {
		So(Select().From(cf).CQL().String(), ShouldEqual, "SELECT X, Y, Z FROM test")
		So(Select("*").From(cf).CQL().String(), ShouldEqual, "SELECT X, Y, Z FROM test")
		So(Select("X").From(cf).CQL().String(), ShouldEqual, "SELECT X FROM test")
		So(Select("X", "Y").From(cf).CQL().String(), ShouldEqual, "SELECT X, Y FROM test")
	})

	Convey("SelectBuilder specifies where conditions correctly", t, func() {
		cql := Select().From(cf).Where("X = ?", 1).CQL()
		So(cql.String(), ShouldEqual, "SELECT X, Y, Z FROM test WHERE X = ?")
		So(cql.params, ShouldResemble, []interface{}{1})

		cql = Select().From(cf).Where("X = ?", 1).Where("Y = 2").Where("Z = ?", 3).CQL()
		So(cql.String(), ShouldEqual, "SELECT X, Y, Z FROM test WHERE X = ? AND Y = 2 AND Z = ?")
		So(cql.params, ShouldResemble, []interface{}{1, 3})
	})

	Convey("SelectBuilder specifies order terms correctly", t, func() {
		So(Select().From(cf).OrderBy("X").CQL().String(),
			ShouldEqual, "SELECT X, Y, Z FROM test ORDER BY X")
		So(Select().From(cf).OrderBy("X").OrderBy("Y DESC").CQL().String(),
			ShouldEqual, "SELECT X, Y, Z FROM test ORDER BY X, Y DESC")
	})

	Convey("SelectBuilder specifies limit correctly", t, func() {
		So(Select().From(cf).Limit(10).CQL().String(),
			ShouldEqual, "SELECT X, Y, Z FROM test LIMIT 10")
		cql := Select("X").From(cf).Where("X = ?", 1).Where("Y < ?", 2).
			OrderBy("X").OrderBy("Y").Limit(1).CQL()
		So(cql.String(),
			ShouldEqual, "SELECT X FROM test WHERE X = ? AND Y < ? ORDER BY X, Y LIMIT 1")
		So(cql.params, ShouldResemble, []interface{}{1, 2})
	})
}

func TestInsertBuilder(t *testing.T) {
	cf := &CF{name: "test"}

	Convey("InsertBuilder builds correctly", t, func() {
		cql := InsertInto(cf).Keys("X").Values(1).CQL()
		So(cql.String(), ShouldEqual, "INSERT INTO test (X) VALUES (?)")
		So(cql.params, ShouldResemble, []interface{}{1})

		cql = InsertInto(cf).Keys("X", "Y").Values(1, 2).IfNotExists().CQL()
		So(cql.String(), ShouldEqual, "INSERT INTO test (X, Y) VALUES (?, ?) IF NOT EXISTS")
		So(cql.params, ShouldResemble, []interface{}{1, 2})
	})
}

func TestUpdateBuilder(t *testing.T) {
	cf := &CF{name: "test"}

	Convey("UpdateBuilder builds correctly", t, func() {
		cql := Update(cf).Set("X", 1).CQL()
		So(cql.String(), ShouldEqual, "UPDATE test SET X = ?")
		So(cql.params, ShouldResemble, []interface{}{1})

		cql = Update(cf).Set("X", 1).Set("Y", 2).Where("X = ?", 3).CQL()
		So(cql.String(), ShouldEqual, "UPDATE test SET X = ?, Y = ? WHERE X = ?")
		So(cql.params, ShouldResemble, []interface{}{1, 2, 3})

		cql = Update(cf).Set("X", 1).Set("Y", 2).Where("X = ?", 3).Where("Y > 0").CQL()
		So(cql.String(), ShouldEqual, "UPDATE test SET X = ?, Y = ? WHERE X = ? AND Y > 0")
		So(cql.params, ShouldResemble, []interface{}{1, 2, 3})
	})
}

func TestDeleteBuilder(t *testing.T) {
	cf := &CF{name: "test"}

	Convey("DeleteBuilder builds correctly", t, func() {
		cql := DeleteFrom(cf).Where("X = ?", 1).CQL()
		So(cql.String(), ShouldEqual, "DELETE FROM test WHERE X = ?")
		So(cql.params, ShouldResemble, []interface{}{1})

		cql = DeleteFrom(cf).Where("X = ?", 1).Where("Y = 2").CQL()
		So(cql.String(), ShouldEqual, "DELETE FROM test WHERE X = ? AND Y = 2")
		So(cql.params, ShouldResemble, []interface{}{1})
	})
}
