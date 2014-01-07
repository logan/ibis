package datastore

import "encoding/json"
import "fmt"
import "strings"

import "tux21b.org/v1/gocql"

type SchemaDiff struct {
	Creations   []*Table
	Alterations []TableAlteration
}

func (d *SchemaDiff) Size() int {
	return len(d.Creations) + len(d.Alterations)
}

func (d *SchemaDiff) String() string {
	if d.Size() == 0 {
		return "no diff"
	}
	changes := make([]string, 0, d.Size())
	for _, t := range d.Creations {
		changes = append(changes, t.CreateStatement())
	}
	for _, a := range d.Alterations {
		changes = append(changes, a.AlterStatements()...)
	}
	return strings.Join(changes, "\n")
}

func (d *SchemaDiff) Apply(session *gocql.Session) error {
	for _, t := range d.Creations {
		if err := session.Query(t.CreateStatement()).Exec(); err != nil {
			return err
		}
	}
	for _, a := range d.Alterations {
		for _, s := range a.AlterStatements() {
			if err := session.Query(s).Exec(); err != nil {
				return err
			}
		}
	}
	return nil
}

type TableAlteration struct {
	TableName      string
	NewColumns     []Column
	AlteredColumns []Column
}

func (a TableAlteration) Size() int {
	return len(a.NewColumns) + len(a.AlteredColumns)
}

func (a TableAlteration) AlterStatements() []string {
	alts := make([]string, 0, a.Size())
	for _, col := range a.NewColumns {
		alts = append(alts, fmt.Sprintf("ALTER TABLE %s ADD %s %s",
			a.TableName, col.Name, col.Type))
	}
	for _, col := range a.AlteredColumns {
		alts = append(alts, fmt.Sprintf("ALTER TABLE %s ALTER %s TYPE %s",
			a.TableName, col.Name, col.Type))
	}
	return alts
}

var column_validators = map[string]string{
	"org.apache.cassandra.db.marshal.BooleanType":   "boolean",
	"org.apache.cassandra.db.marshal.DoubleType":    "double",
	"org.apache.cassandra.db.marshal.LongType":      "long",
	"org.apache.cassandra.db.marshal.TimestampType": "timestamp",
	"org.apache.cassandra.db.marshal.UTF8Type":      "varchar",
}

func (c *CassandraConn) GetLiveSchema() (*Schema, error) {
	schema := Schema{make(map[string]*Table)}
	var err error
	tables, err := getLiveColumnFamilies(c.Session, c.Config.Keyspace)
	if err != nil {
		return nil, err
	}
	for _, t := range tables {
		schema.Tables[strings.ToLower(t.Name)] = t
	}
	q := c.Session.Query(
		`SELECT columnfamily_name, column_name, validator FROM system.schema_columns
             WHERE keyspace_name = ?`, c.Config.Keyspace)
	i := q.Iter()
	var cf_name, col_name, validator string
	for i.Scan(&cf_name, &col_name, &validator) {
		col := Column{col_name, typeFromValidator(validator)}
		t := schema.Tables[cf_name]
		t.Columns = append(t.Columns, col)
	}
	return &schema, i.Close()
}

func getLiveColumnFamilies(session *gocql.Session, keyspace string) ([]*Table, error) {
	q := session.Query(
		`SELECT columnfamily_name, key_aliases, column_aliases FROM system.schema_columnfamilies
             WHERE keyspace_name = ?`, keyspace)
	tables := make([]*Table, 0, 32)
	var cf_name, key_aliases, column_aliases string
	i := q.Iter()
	for i.Scan(&cf_name, &key_aliases, &column_aliases) {
		o := TableOptions{keyFromAliases(key_aliases, column_aliases)}
		t := Table{cf_name, make([]Column, 0, 16), o}
		tables = append(tables, &t)
	}
	return tables, i.Close()
}

func typeFromValidator(validator string) string {
	type_name, ok := column_validators[validator]
	if !ok {
		type_name = "blob"
	}
	return type_name
}

func parseStringList(encoded string) []string {
	var result []string
	json.Unmarshal([]byte(encoded), &result)
	return result
}

func keyFromAliases(key_aliases, column_aliases string) []string {
	return append(parseStringList(key_aliases), parseStringList(column_aliases)...)
}

func (c *CassandraConn) DiffLiveSchema() (*SchemaDiff, error) {
	var live *Schema
	var err error
	if live, err = c.GetLiveSchema(); err != nil {
		return nil, err
	}
	var diff = &SchemaDiff{make([]*Table, 0), make([]TableAlteration, 0)}
	for name, model_table := range c.Model.Tables {
		live_table, ok := live.Tables[strings.ToLower(name)]
		if ok {
			alteration := TableAlteration{name, make([]Column, 0), make([]Column, 0)}
			old_cols := make(map[string]string)
			for _, col := range live_table.Columns {
				old_cols[strings.ToLower(col.Name)] = col.Type
			}
			for _, col := range model_table.Columns {
				var old_type string
				if old_type, ok = old_cols[strings.ToLower(col.Name)]; ok {
					if old_type != col.Type {
						alteration.AlteredColumns = append(alteration.AlteredColumns, col)
					}
				} else {
					alteration.NewColumns = append(alteration.NewColumns, col)
				}
			}
			if len(alteration.NewColumns)+len(alteration.AlteredColumns) > 0 {
				diff.Alterations = append(diff.Alterations, alteration)
			}
		} else {
			diff.Creations = append(diff.Creations, model_table)
		}
	}
	return diff, nil
}

func (c *CassandraConn) ApplySchemaUpdates() error {
	return c.SchemaUpdates.Apply(c.Session)
}
