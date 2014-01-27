package ibis

import "reflect"
import "strings"

type Keyspace map[string]*ColumnFamily

// Schema is a map of column families by name, defining a keyspace.
type Schema struct {
	Cluster
	CFs           Keyspace
	SchemaUpdates *SchemaDiff
	nextTypeID    int
}

// NewSchema returns a new, empty schema.
func NewSchema() *Schema {
	return &Schema{
		CFs:        make(Keyspace),
		nextTypeID: 1,
	}
}

// AddCF adds a column family definition to the schema.
func (s *Schema) AddCF(cf *ColumnFamily) {
	s.CFs[strings.ToLower(cf.Name)] = cf
	cf.Schema = s
	if cf.typeID == 0 {
		cf.typeID = s.nextTypeID
		s.nextTypeID++
	}
}

func (s *Schema) DialCassandra(config CassandraConfig) error {
	cluster, err := DialCassandra(config)
	if err != nil {
		return err
	}
	s.Cluster = cluster
	s.SchemaUpdates, err = DiffLiveSchema(s.Cluster, s)
	return err
}

// RequiresUpdates returns true if the Orm model differs from the existing column families in
// Cassandra.
func (s *Schema) RequiresUpdates() bool {
	return s.SchemaUpdates.Size() > 0
}

// ApplySchemaUpdates applies any required modifications to the live schema to match the Orm model.
func (s *Schema) ApplySchemaUpdates() error {
	return s.SchemaUpdates.Apply(s.Cluster)
}

// IsBound returns true if the schema is bound to an *Orm.
func (s *Schema) IsBound() bool {
	return s.Cluster != nil
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
	providerType := reflect.TypeOf((*CFProvider)(nil)).Elem()
	idGenType := reflect.TypeOf((*SeqIDGenerator)(nil)).Elem()
	schema := NewSchema()

	var idGen SeqIDGenerator
	for i := 0; i < model_type.NumField(); i++ {
		field := model_type.Field(i)
		if field.PkgPath != "" {
			// non-empty PkgPath indicates unexported field, do not reflect these
			continue
		}
		field_value := model_value.FieldByIndex(field.Index)
		if field.Type.ConvertibleTo(providerType) {
			if provider, ok := field_value.Interface().(CFProvider); ok {
				if field_value.IsNil() {
					field_value.Set(reflect.New(field.Type.Elem()))
					provider = field_value.Interface().(CFProvider)
				}
				cf := provider.CF()
				cf.Name = strings.ToLower(field.Name)
				if cf.rowReflector != nil {
					cf.fillFromRowType(cf.rowReflector.rowType.Elem())
				}
				schema.AddCF(cf)
			}
		} else if field.Type.ConvertibleTo(idGenType) {
			idGen = field_value.Interface().(SeqIDGenerator)
		}
	}
	if idGen != nil {
		for _, cf := range schema.CFs {
			cf.SeqIDGenerator = idGen
		}
	}
	return schema
}
