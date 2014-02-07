package ibis

import "reflect"
import "testing"

import . "github.com/smartystreets/goconvey/convey"

type row struct {
	Str string `ibis:"key"`
	Int int64
}

type table struct {
	*CF
}

func (t *table) NewCF() (*CF, error) {
	var err error
	t.CF, err = ReflectCF(row{})
	return t.cf, err
}

func TestSchemaMiscellanea(t *testing.T) {
	Convey("IsBound", t, func() {
		schema := NewSchema()
		So(schema.IsBound(), ShouldBeFalse)
		schema.Cluster = NewTestConn(t)
		So(schema.IsBound(), ShouldBeTrue)
	})

	Convey("RequiresUpdates", t, func() {
		var (
			m   struct{ T *table }
			err error
		)
		cluster := NewTestConn(t)
		schema, err := ReflectSchema(&m)
		So(err, ShouldBeNil)
		So(schema.RequiresUpdates(), ShouldBeFalse)

		schema.SchemaUpdates, err = DiffLiveSchema(cluster, schema)
		So(err, ShouldBeNil)
		So(schema.RequiresUpdates(), ShouldBeTrue)

		schema.Cluster = cluster
		err = schema.ApplySchemaUpdates()
		So(err, ShouldBeNil)

		schema, err = ReflectSchema(&m)
		So(err, ShouldBeNil)
		schema.SchemaUpdates, err = DiffLiveSchema(cluster, schema)

		So(err, ShouldBeNil)
		So(schema.RequiresUpdates(), ShouldBeFalse)
	})
}

func TestReflectSchema(t *testing.T) {
	type model struct {
		Defined    *CF
		Reflected  *table
		unexported *table // having this here shouldn't break anything
	}

	m := &model{}
	expectedColumns := []Column{
		Column{Name: "Str", Type: "varchar", typeInfo: TIVarchar},
		Column{Name: "Int", Type: "bigint", typeInfo: TIBigInt},
	}
	m.Defined = NewCF("Defined", expectedColumns...).SetPrimaryKey("Str")
	schema, err := ReflectSchema(m)
	if err != nil {
		t.Fatal(err)
	}
	expectedPrimaryKey := []string{"Str"}

	Convey("Boilerplate-y table definition", t, func() {
		cf, ok := schema.CFs["defined"]
		So(ok, ShouldBeTrue)
		So(cf.primaryKey, ShouldResemble, expectedPrimaryKey)
		So(cf.columns, ShouldResemble, expectedColumns)
	})

	Convey("Reflected table definition", t, func() {
		cf, ok := schema.CFs["reflected"]
		So(ok, ShouldBeTrue)
		So(cf.primaryKey, ShouldResemble, expectedPrimaryKey)
		cols := make([]Column, len(expectedColumns))
		for i, col := range expectedColumns {
			cols[i] = col
		}
		cols[0].tag = reflect.StructTag(`ibis:"key"`)
		So(cf.columns, ShouldResemble, cols)
	})

	Convey("Should panic on invalid argument", t, func() {
		_, err := ReflectSchema(8)
		So(err, shouldBeError, ErrInvalidSchemaType)
		_, err = ReflectSchema(model{})
		So(err, shouldBeError, ErrInvalidSchemaType)
	})
}

type providerTester interface {
	Test() string
}
type providerTest string

func (t providerTest) Test() string { return string(t) }

func TestGetProviderFromSchema(t *testing.T) {
	Convey("GetProvider should fall back to schema", t, func() {
		cf := NewCF("test")
		schema := NewSchema()
		schema.AddCF(cf)

		var p providerTester
		So(schema.GetProvider(&p), ShouldBeFalse)

		schema.Provide(providerTester(providerTest("schema")))
		So(schema.GetProvider(&p), ShouldBeTrue)
		So(p.Test(), ShouldEqual, "schema")

		cf.Provide(providerTester(providerTest("cf")))
		So(schema.GetProvider(&p), ShouldBeTrue)
		So(p.Test(), ShouldEqual, "cf")

		var c Cluster
		So(schema.GetProvider(&c), ShouldBeFalse)
	})

	Convey("Incorrect type should raise panic", t, func() {
		schema := NewSchema()
		So(func() { schema.GetProvider(1) }, ShouldPanicWith,
			"destination must be a pointer to an interface")
		So(func() { schema.GetProvider(new(providerTest)) }, ShouldPanicWith,
			"destination must be a pointer to an interface")
	})
}

type pluginTest bool

func (p *pluginTest) RegisterColumnTags(tags *ColumnTags) { *p = true }

func TestAddCF(t *testing.T) {
	Convey("AddCF should register plugins", t, func() {
		p := new(pluginTest)
		cf := NewCF("test")
		cf.Provide(SchemaPlugin(p))
		So(bool(*p), ShouldBeFalse)

		schema := NewSchema()
		schema.AddCF(cf)
		So(bool(*p), ShouldBeTrue)
	})
}
