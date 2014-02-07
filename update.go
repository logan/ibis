package ibis

import "encoding/json"
import "strconv"
import "strings"

// SchemaDiff enumerates the changes necessary to transform one schema into another.
type SchemaDiff struct {
	creations   []*CF             // tables that are completely missing from the former schema
	alterations []tableAlteration // tables that have missing or altered columns
}

// Size returns the total number of creations and alterations in the SchemaDiff.
func (d *SchemaDiff) Size() int {
	return len(d.creations) + len(d.alterations)
}

// String constructs a human-readable string describing the SchemaDiff in CQL.
func (d *SchemaDiff) String() string {
	if d.Size() == 0 {
		return "no diff"
	}
	changes := make([]string, 0, d.Size())
	for _, t := range d.creations {
		changes = append(changes, t.CreateStatement().String())
	}
	for _, a := range d.alterations {
		for _, cql := range a.AlterStatements() {
			changes = append(changes, cql.String())
		}
	}
	return strings.Join(changes, "\n")
}

// Apply issues CQL statements to transform the former schema into the latter.
func (d *SchemaDiff) Apply(cluster Cluster) error {
	for _, t := range d.creations {
		cql := t.CreateStatement()
		cql.Cluster(cluster)
		if err := cql.Query().Exec(); err != nil {
			return ChainError(err, "column family creation failed")
		}
	}
	for _, a := range d.alterations {
		for _, s := range a.AlterStatements() {
			s.Cluster(cluster)
			if err := s.Query().Exec(); err != nil {
				return ChainError(err, "column alteration failed")
			}
		}
	}
	return nil
}

type tableAlteration struct {
	TableName      string
	NewColumns     []Column
	AlteredColumns []Column
}

func (a tableAlteration) Size() int {
	return len(a.NewColumns) + len(a.AlteredColumns)
}

func (a tableAlteration) AlterStatements() []CQL {
	alts := make([]CQL, 0, a.Size())
	for _, col := range a.NewColumns {
		var b CQLBuilder
		b.Append("ALTER TABLE ").Append(a.TableName).
			Append(" ADD ").Append(col.Name + " " + col.Type)
		alts = append(alts, b.CQL())
	}
	for _, col := range a.AlteredColumns {
		var b CQLBuilder
		b.Append("ALTER TABLE ").Append(a.TableName).
			Append(" ALTER ").Append(col.Name).Append(" TYPE ").Append(col.Type)
		alts = append(alts, b.CQL())
	}
	return alts
}

// GetLiveSchema builds a schema by querying the column families that exist in the current keyspace
// of the given cluster.
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
		schema.CFs[strings.ToLower(t.name)] = t
	}
	sel := Select("columnfamily_name", "column_name", "validator").
		From(NewCF("system.schema_columns")).Where("keyspace_name = ?", c.GetKeyspace())
	cql := sel.CQL()
	cql.Cluster(c)
	qiter := cql.Query()
	var cf_name, col_name, validator string
	for qiter.Scan(&cf_name, &col_name, &validator) {
		col := Column{Name: col_name, Type: typeFromValidator(validator)}
		t := schema.CFs[cf_name]
		if t != nil {
			t.columns = append(t.columns, col)
		}
	}
	for _, cf := range schema.CFs {
		// reapply primary key to fix column ordering
		cf.SetPrimaryKey(cf.primaryKey...)
	}
	return &schema, qiter.Close()
}

func getLiveColumnFamilies(cluster Cluster, keyspace string) ([]*CF, int, error) {
	cf := &CF{name: "system.schema_columnfamilies"}
	sel := Select("columnfamily_name", "key_aliases", "column_aliases", "comment").
		From(cf).Where("keyspace_name = ?", keyspace)
	cql := sel.CQL()
	cql.Cluster(cluster)
	qiter := cql.Query()
	tables := make([]*CF, 0, 32)
	var cf_name, key_aliases, column_aliases, comment string
	maxTypeID := 0
	for qiter.Scan(&cf_name, &key_aliases, &column_aliases, &comment) {
		t := CF{name: cf_name, columns: make([]Column, 0, 16)}
		t.SetPrimaryKey(keyFromAliases(key_aliases, column_aliases)...)
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
		if model_t, ok := model.CFs[t.name]; ok {
			model_t.typeID = t.typeID
		}
	}
	var diff = &SchemaDiff{make([]*CF, 0), make([]tableAlteration, 0)}
	for name, model_table := range model.CFs {
		live_table, ok := live.CFs[strings.ToLower(name)]
		if ok {
			alteration := tableAlteration{name, make([]Column, 0), make([]Column, 0)}
			old_cols := make(map[string]string)
			for _, col := range live_table.columns {
				old_cols[strings.ToLower(col.Name)] = col.Type
			}
			for _, col := range model_table.columns {
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
				diff.alterations = append(diff.alterations, alteration)
			}
		} else {
			model_table.typeID = model.nextTypeID
			model.nextTypeID++
			diff.creations = append(diff.creations, model_table)
		}
	}
	return diff, nil
}
