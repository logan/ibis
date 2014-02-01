package ibis

import "reflect"
import "strings"

type Keyspace map[string]*CF

// Schema is a map of column families by name, defining a keyspace.
type Schema struct {
	Cluster
	CFs           Keyspace
	SchemaUpdates *SchemaDiff
	nextTypeID    int
}

// NewSchema returns an empty, unbound schema.
func NewSchema() *Schema {
	return &Schema{
		CFs:        make(Keyspace),
		nextTypeID: 1,
	}
}

// AddCF adds a column family definition to the schema.
func (s *Schema) AddCF(cf *CF) {
	s.CFs[strings.ToLower(cf.name)] = cf
	cf.schema = s
	cf.Cluster = s.Cluster
	if cf.typeID == 0 {
		cf.typeID = s.nextTypeID
		s.nextTypeID++
	}
}

// GetProvider checks all of this schema's column families for a provider that implements the
// interface pointed to by dest. A panic will occur if dest is not a pointer to an interface.
// If multiple compatible providers are registered across the schema, an arbitrary one will be
// used. If a compatible provider is found, it is copied into *dest and true is returned. Otherwise
// false is returned.
func (s *Schema) GetProvider(dest interface{}) bool {
	for _, cf := range s.CFs {
		if cf.GetProvider(dest) {
			return true
		}
	}
	return false
}

// DialCassandra uses gocql to connect to a Cassandra cluster and binds this schema to it.
//
// Upon connecting, the live schema is automatically scanned and compared to this one. The
// difference between the two will be available in the SchemaUpdates field, and the
// RequiresUpdates method will be able to report on whether a difference exists.
func (s *Schema) DialCassandra(config CassandraConfig) error {
	cluster, err := DialCassandra(config)
	if err != nil {
		return err
	}
	s.SetCluster(cluster)
	s.SchemaUpdates, err = DiffLiveSchema(s.Cluster, s)
	return err
}

// SetCluster connects a schema (and all of its column families) to a Cluster.
func (s *Schema) SetCluster(cluster Cluster) {
	s.Cluster = cluster
	for _, cf := range s.CFs {
		cf.Cluster = cluster
	}
}

// RequiresUpdates returns true if this schema differs from the existing column families in the
// connected cluster.
func (s *Schema) RequiresUpdates() bool {
	return s.SchemaUpdates.Size() > 0
}

// ApplySchemaUpdates applies any required modifications to the live schema to match this one.
func (s *Schema) ApplySchemaUpdates() error {
	return s.SchemaUpdates.Apply(s.Cluster)
}

// IsBound returns true if the schema is bound to a Cluster.
func (s *Schema) IsBound() bool {
	return s.Cluster != nil
}

// ReflectSchema uses reflection to derive a schema from the fields of a given struct.
//
// The returned schema will include a column family for each exported field of CFProvider type.
// The name of the column family will be the lowercased name of the corresponding field.
//
// CFProvider fields that are nil pointers will be initialized with a new value. They must be
// capable of producing their column family definition from a zero value.
//
//       type User struct {Name string, Password string}
//       type UserTable struct {*ibis.CF}
//       func (t *UserTable) CF() *ibis.CF {
//           t.CF = ibis.ReflectCF(User{})
//           return t.Key("Name")
//       }
//       type Model struct{Users *UserTable}
//       model := &Model{}
//       schema := ibis.ReflectSchemaFrom(model)
//
func ReflectSchema(model interface{}) *Schema {
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
	schema := NewSchema()

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
				cf := provider.NewCF()
				cf.name = strings.ToLower(field.Name)
				if cf.rowReflector != nil {
					cf.fillFromRowType(cf.rowReflector.rowType.Elem())
				}
				schema.AddCF(cf)
			}
		}
	}
	return schema
}
