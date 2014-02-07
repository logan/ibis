package ibis

import "testing"

import . "github.com/smartystreets/goconvey/convey"

type testSchemaPlugin struct {
	tagValue  string
	taggedCF  *CF
	taggedCol Column
}

func (p *testSchemaPlugin) RegisterColumnTags(tags *ColumnTags) {
	tags.Register("test", p)
}

func (p *testSchemaPlugin) ApplyTag(tagValue string, cf *CF, col Column) error {
	p.tagValue = tagValue
	p.taggedCF = cf
	p.taggedCol = col
	return nil
}

func TestColumnTags(t *testing.T) {
	type Row struct {
		Key   string `ibis:"key"`
		Thing string `ibis.test:"thing"`
	}

	plugin := &testSchemaPlugin{}
	pluginCF := NewCF("plugin")
	pluginCF.Provide(SchemaPlugin(plugin))

	cf, err := ReflectCF(Row{})
	if err != nil {
		t.Fatal(err)
	}

	schema := NewSchema()
	schema.AddCF(pluginCF)
	schema.AddCF(cf)

	Convey("ColumnTags", t, func() {
		So(plugin.tagValue, ShouldEqual, "thing")
	})
}

func TestAutoPatcher(t *testing.T) {
	type Row struct {
		*AutoPatcher
		K string `ibis:"key"`
		V string
		W string
	}

	cf, err := ReflectCF(Row{})
	if err != nil {
		t.Fatal(err)
	}
	schema := ReflectTestSchema(t, &struct{ T *CF }{cf})
	defer schema.Cluster.Close()

	Convey("AutoPatcher should remember loaded/committed values", t, func() {
		row := &Row{K: "x", V: "y"}
		So(row.AutoPatcher, ShouldBeNil)
		So(cf.Commit(row), ShouldBeNil)
		So(row.AutoPatcher, ShouldNotBeNil)

		expected := AutoPatcher{
			"K": LiteralValue("x").MarkClean(),
			"V": LiteralValue("y").MarkClean(),
			"W": LiteralValue("").MarkClean(),
		}
		So(*row.AutoPatcher, ShouldResemble, expected)
	})

	Convey("OnMarshal should fill in OriginalBytes before commit", t, func() {
		row := &Row{K: "x", V: "y"}
		So(cf.Commit(row), ShouldBeNil)
		So(row.AutoPatcher, ShouldNotBeNil)

		mmap := MarshaledMap{"K": LiteralValue("x"), "V": LiteralValue("y2")}
		So(row.AutoPatcher.OnMarshal(mmap), ShouldBeNil)
		So(mmap.DirtyKeys(), ShouldResemble, []string{"V"})
	})

	Convey("Verify isolation of AutoPatcher commits", t, func() {
		row := &Row{K: "x", V: "v1", W: "w1"}
		So(cf.Commit(row), ShouldBeNil)

		var row1, row2 Row
		So(cf.LoadByKey(&row1, "x"), ShouldBeNil)
		So(row1.AutoPatcher, ShouldNotBeNil)
		So(cf.LoadByKey(&row2, "x"), ShouldBeNil)
		So(row2.AutoPatcher, ShouldNotBeNil)
		row1.V = "v2"
		row2.W = "w2"
		So(cf.Commit(&row1), ShouldBeNil)
		So(cf.Commit(&row2), ShouldBeNil)

		So(cf.LoadByKey(row, "x"), ShouldBeNil)
		So(*row, ShouldResemble, Row{AutoPatcher: row.AutoPatcher, K: "x", V: "v2", W: "w2"})
	})
}
