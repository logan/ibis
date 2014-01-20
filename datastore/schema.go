package datastore

import "reflect"
import "strings"

type Keyspace map[string]*ColumnFamily

// Schema is a map of column families by name, defining a keyspace.
type Schema struct {
	CFs        Keyspace
	nextTypeID int
	orm        *Orm
}

// NewSchema returns a new, empty schema.
func NewSchema() *Schema {
	return &Schema{CFs: make(Keyspace), nextTypeID: 1}
}

// AddCF adds a column family definition to the schema.
func (s *Schema) AddCF(cf *ColumnFamily) {
	s.CFs[strings.ToLower(cf.Name)] = cf
	for _, val := range cf.Options.ctx {
		if idx, ok := val.(CFIndex); ok {
			for _, subcf := range idx.CFs() {
				s.AddCF(subcf)
			}
		}
	}
	if cf.Options.typeID == 0 {
		cf.Options.typeID = s.nextTypeID
		s.nextTypeID++
	}
}

// Bind returns a new schema with all CFs bound to the given *Orm.
func (s *Schema) Bind(orm *Orm) {
	for _, cf := range s.CFs {
		cf.Bind(orm)
	}
}

// IsBound returns true if the schema is bound to an *Orm.
func (s *Schema) IsBound() bool {
	return s.orm != nil
}

type ReflectableColumnFamily interface {
	ConfigureCF(*CFOptions)
	NewRow() Persistable
}

func ReflectSchemaFrom(model interface{}) *Schema {
	ptr_type := reflect.TypeOf(model)
	if ptr_type.Kind() != reflect.Ptr {
		panic("model must be pointer to struct")
	}
	model_value := reflect.Indirect(reflect.ValueOf(model))
	model_type := model_value.Type()
	if model_type.Kind() != reflect.Struct {
		panic("model must be pointer to struct")
	}
	rcf_type := reflect.TypeOf((*ReflectableColumnFamily)(nil)).Elem()
	schema := NewSchema()
	for i := 0; i < model_type.NumField(); i++ {
		field := model_type.Field(i)
		field_value := reflect.New(field.Type.Elem())
		if field.Type.Implements(rcf_type) {
			if rcf, ok := field_value.Interface().(ReflectableColumnFamily); ok {
				cf := &ColumnFamily{}
				cf.Options = NewCFOptions(cf)
				row := rcf.NewRow()
				cf.fillFromRowType(field.Name, reflect.TypeOf(row))
				rcf.ConfigureCF(cf.Options)
				schema.AddCF(cf)
				cf_value := reflect.ValueOf(cf).Convert(field.Type)
				model_value.FieldByIndex(field.Index).Set(cf_value)
			}
		}
	}
	return schema
}
