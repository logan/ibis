package datastore

import "encoding/json"
import "fmt"
import "strconv"
import "strings"

// A SchemaDiff enumerates the changes necessary to transform one schema into another.
type SchemaDiff struct {
	Creations   []*ColumnFamily   // tables that are completely missing from the former schema
	Alterations []TableAlteration // tables that have missing or altered columns
}

// Size returns the total number of creations and alterations in the SchemaDiff.
func (d *SchemaDiff) Size() int {
	return len(d.Creations) + len(d.Alterations)
}

// String constructs a human-readable string describing the SchemaDiff in CQL.
func (d *SchemaDiff) String() string {
	if d.Size() == 0 {
		return "no diff"
	}
	changes := make([]string, 0, d.Size())
	for _, t := range d.Creations {
		changes = append(changes, t.CreateStatement().String())
	}
	for _, a := range d.Alterations {
		for _, cql := range a.AlterStatements() {
			changes = append(changes, cql.String())
		}
	}
	return strings.Join(changes, "\n")
}

// Apply issues CQL statements to transform the former schema into the latter.
func (d *SchemaDiff) Apply(cluster Cluster) error {
	for _, t := range d.Creations {
		if err := cluster.Query(t.CreateStatement()).Exec(); err != nil {
			return err
		}
		for _, hook := range t.onCreateHooks {
			if err := hook(t); err != nil {
				return err
			}
		}
	}
	for _, a := range d.Alterations {
		for _, s := range a.AlterStatements() {
			if err := cluster.Query(s).Exec(); err != nil {
				return err
			}
		}
	}
	return nil
}

// TableAlteration describes a set of column additions and alterations for a single table.
type TableAlteration struct {
	TableName      string
	NewColumns     []Column
	AlteredColumns []Column
}

// Size returns the total number of new and altered columns.
func (a TableAlteration) Size() int {
	return len(a.NewColumns) + len(a.AlteredColumns)
}

// AlterStatements generates a list of CQL statements, one for each new or altered column.
func (a TableAlteration) AlterStatements() []*CQL {
	alts := make([]*CQL, 0, a.Size())
	for _, col := range a.NewColumns {
		cql := NewCQL(fmt.Sprintf("ALTER TABLE %s ADD %s %s", a.TableName, col.Name, col.Type))
		alts = append(alts, cql)
	}
	for _, col := range a.AlteredColumns {
		cql := NewCQL(fmt.Sprintf("ALTER TABLE %s ALTER %s TYPE %s",
			a.TableName, col.Name, col.Type))
		alts = append(alts, cql)
	}
	return alts
}

// GetLiveSchema builds a schema by querying the column families that exist in the connected
// keyspace.
func GetLiveSchema(c Cluster) (*Schema, error) {
	var err error
	tables, nextTypeID, err := getLiveColumnFamilies(c, c.GetKeyspace())
	if err != nil {
		return nil, err
	}
	schema := Schema{
		CFs:        make(Keyspace),
		nextTypeID: nextTypeID,
	}
	for _, t := range tables {
		schema.CFs[strings.ToLower(t.Name)] = t
	}
	cf := &ColumnFamily{Name: "system.schema_columns"}
	cql := NewSelect(cf)
	cql.Cols("columnfamily_name", "column_name", "validator")
	cql.Where("keyspace_name = ?", c.GetKeyspace())
	qiter := c.Query(cql)
	var cf_name, col_name, validator string
	for qiter.Scan(&cf_name, &col_name, &validator) {
		col := Column{Name: col_name, Type: typeFromValidator(validator)}
		t := schema.CFs[cf_name]
		if t != nil {
			t.Columns = append(t.Columns, col)
		}
	}
	for _, cf := range schema.CFs {
		// reapply primary key to fix column ordering
		cf.Key(cf.PrimaryKey...)
	}
	return &schema, qiter.Close()
}

func getLiveColumnFamilies(cluster Cluster, keyspace string) ([]*ColumnFamily, int, error) {
	cf := &ColumnFamily{Name: "system.schema_columnfamilies"}
	cql := NewSelect(cf)
	cql.Cols("columnfamily_name", "key_aliases", "column_aliases", "comment")
	cql.Where("keyspace_name = ?", keyspace)
	qiter := cluster.Query(cql)
	tables := make([]*ColumnFamily, 0, 32)
	var cf_name, key_aliases, column_aliases, comment string
	maxTypeID := 0
	for qiter.Scan(&cf_name, &key_aliases, &column_aliases, &comment) {
		t := ColumnFamily{Name: cf_name, Columns: make([]Column, 0, 16)}
		t.Key(keyFromAliases(key_aliases, column_aliases)...)
		t.typeID = typeIDFromComment(comment)
		if t.typeID > maxTypeID {
			maxTypeID = t.typeID
		}
		tables = append(tables, &t)
	}
	return tables, maxTypeID + 1, qiter.Close()
}

func typeIDFromComment(comment string) int {
	i, err := strconv.Atoi(comment)
	if err != nil {
		return 0
	}
	return i
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

// DiffLiveSchema compares the current schema in Cassandra to the given model. It returns a pointer
// to a SchemaDiff describing the differences. If the two schemas are identical, then this
// SchemaDiff will be empty.
//
// This function also modifies the given model, to fix typeIDs of tables that exist in Cassandra.
func DiffLiveSchema(c Cluster, model *Schema) (*SchemaDiff, error) {
	var live *Schema
	var err error
	if live, err = GetLiveSchema(c); err != nil {
		return nil, err
	}
	for _, t := range live.CFs {
		if t.typeID >= model.nextTypeID {
			model.nextTypeID = t.typeID + 1
		}
		if model_t, ok := model.CFs[t.Name]; ok {
			model_t.typeID = t.typeID
		}
	}
	var diff = &SchemaDiff{make([]*ColumnFamily, 0), make([]TableAlteration, 0)}
	for name, model_table := range model.CFs {
		live_table, ok := live.CFs[strings.ToLower(name)]
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
			model_table.typeID = model.nextTypeID
			model.nextTypeID++
			diff.Creations = append(diff.Creations, model_table)
		}
	}
	return diff, nil
}
