package datastore

import "errors"
import "fmt"
import "reflect"
import "testing"

func parseInto(s string, dest interface{}) error {
	tok := parseWith(s, pStatement)
	if tok.err != nil {
		return tok.err
	}
	destval := reflect.ValueOf(dest)
	if destval.Type().Kind() != reflect.Ptr {
		return errors.New("parseInto dest requires a pointer")
	}
	elem := destval.Elem()
	ctx := reflect.ValueOf(tok.ctx).Elem()
	if elem.Type() != ctx.Type() {
		return errors.New(fmt.Sprintf("dest type %s incompatible with ctx type %s",
			elem.Type(), ctx.Type()))
	}
	elem.Set(ctx)
	return nil
}

func TestParseCreateKeyspace(t *testing.T) {
	var cmd createKeyspaceCommand
	parse := func(s string) error { return parseInto(s, &cmd) }

	if err := parse("CREATE KEYSPACE test"); err != nil {
		t.Fatal(err)
	}
	if cmd.identifier != "test" {
		t.Errorf("wrong table: %+v", cmd)
	}
	if !cmd.strict {
		t.Errorf("strict should be true: %+v", cmd)
	}
	if len(cmd.options) != 0 {
		t.Errorf("options should be empty: %+v", cmd)
	}

	if err := parse("CREATE KEYSPACE IF NOT EXISTS test"); err != nil {
		t.Fatal(err)
	}
	if cmd.identifier != "test" {
		t.Errorf("wrong table: %+v", cmd)
	}
	if cmd.strict {
		t.Errorf("strict should be false: %+v", cmd)
	}

	if err := parse("CREATE KEYSPACE IF NOT EXISTS test WITH x=1"); err != nil {
		t.Fatal(err)
	}
	if cmd.identifier != "test" {
		t.Errorf("wrong table: %+v", cmd)
	}
	if len(cmd.options) != 1 || cmd.options["x"] == "" {
		t.Errorf("invalid options: %+v", cmd)
	}

	if err := parse("CREATE KEYSPACE test WITH x=1 AND y = 'str'"); err != nil {
		t.Fatal(err)
	}
	if cmd.identifier != "test" {
		t.Errorf("wrong table: %+v", cmd)
	}
	if len(cmd.options) != 2 || cmd.options["x"] == "" || cmd.options["y"] == "" {
		t.Errorf("invalid options: %+v", cmd)
	}
}

func TestParseCreateTable(t *testing.T) {
	var cmd createTableCommand
	parse := func(s string) error { return parseInto(s, &cmd) }

	if err := parse("CREATE TABLE t (x varchar)"); err != nil {
		t.Fatal(err)
	}
	if cmd.identifier != "t" {
		t.Errorf("wrong table: %+v", cmd)
	}
	if len(cmd.colnames) != 1 || cmd.colnames[0] != "x" {
		t.Errorf("wrong cols: %+v", cmd)
	}
	if len(cmd.coltypes) != 1 || cmd.coltypes[0] != TIVarchar {
		t.Errorf("wrong coltypes: %+v", cmd)
	}
	if !cmd.strict {
		t.Errorf("strict should be true: %+v", cmd)
	}

	if err := parse("CREATE TABLE t (x varchar, y blob PRIMARY KEY, z bigint)"); err != nil {
		t.Fatal(err)
	}
	if len(cmd.colnames) != 3 {
		t.Errorf("wrong cols: %+v", cmd)
	}
	if len(cmd.key) != 1 || cmd.key[0] != "y" {
		t.Errorf("wrong key: %+v", cmd)
	}

	if err := parse("CREATE TABLE t (x blob, PRIMARY KEY (x, y), y blob)"); err != nil {
		t.Fatal(err)
	}
	if len(cmd.colnames) != 2 {
		t.Errorf("wrong cols: %+v", cmd)
	}
	if len(cmd.key) != 2 || cmd.key[0] != "x" || cmd.key[1] != "y" {
		t.Errorf("wrong key: %+v", cmd)
	}

	if err := parse("CREATE TABLE IF NOT EXISTS t (x blob PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	if cmd.strict {
		t.Errorf("strict should be false: %+v", cmd)
	}
}

func TestParseDrop(t *testing.T) {
	var cmd dropCommand
	parse := func(s string) error { return parseInto(s, &cmd) }

	if err := parse("DROP KEYSPACE x"); err != nil {
		t.Fatal(err)
	}
	if cmd.dropType != "keyspace" {
		t.Errorf("wrong droptype: %+v", cmd)
	}
	if cmd.identifier != "x" {
		t.Errorf("wrong identifier: %+v", cmd)
	}
	if !cmd.strict {
		t.Errorf("strict should be true: %+v", cmd)
	}

	if err := parse("DROP TABLE x"); err != nil {
		t.Fatal(err)
	}
	if cmd.dropType != "table" {
		t.Errorf("wrong droptype: %+v", cmd)
	}
	if cmd.identifier != "x" {
		t.Errorf("wrong identifier: %+v", cmd)
	}
	if !cmd.strict {
		t.Errorf("strict should be true: %+v", cmd)
	}

	if err := parse("DROP KEYSPACE IF EXISTS x"); err != nil {
		t.Fatal(err)
	}
	if cmd.dropType != "keyspace" {
		t.Errorf("wrong droptype: %+v", cmd)
	}
	if cmd.identifier != "x" {
		t.Errorf("wrong identifier: %+v", cmd)
	}
	if cmd.strict {
		t.Errorf("strict should be false: %+v", cmd)
	}

	if err := parse("DROP TABLE IF EXISTS x"); err != nil {
		t.Fatal(err)
	}
	if cmd.dropType != "table" {
		t.Errorf("wrong droptype: %+v", cmd)
	}
	if cmd.identifier != "x" {
		t.Errorf("wrong identifier: %+v", cmd)
	}
	if cmd.strict {
		t.Errorf("strict should be false: %+v", cmd)
	}
}

func TestParseAlter(t *testing.T) {
	var cmd alterCommand
	parse := func(s string) error { return parseInto(s, &cmd) }

	if err := parse("ALTER TABLE test ALTER x TYPE varchar"); err != nil {
		t.Fatal(err)
	}
	if cmd.table != "test" {
		t.Errorf("wrong table: %+v", cmd)
	}
	if cmd.alter != "x" || cmd.add != "" || cmd.drop != "" {
		t.Errorf("wrong cmd: %+v", cmd)
	}
	if cmd.coltype != TIVarchar {
		t.Errorf("wrong coltype: %+v", cmd)
	}
	if len(cmd.options) != 0 {
		t.Errorf("invalid options: %+v", cmd)
	}

	if err := parse("ALTER TABLE test ADD x varchar"); err != nil {
		t.Fatal(err)
	}
	if cmd.add != "x" || cmd.alter != "" || cmd.drop != "" {
		t.Errorf("wrong cmd: %+v", cmd)
	}
	if cmd.coltype != TIVarchar {
		t.Errorf("wrong coltype: %+v", cmd)
	}
	if len(cmd.options) != 0 {
		t.Errorf("invalid options: %+v", cmd)
	}

	if err := parse("ALTER TABLE test DROP x"); err != nil {
		t.Fatal(err)
	}
	if cmd.drop != "x" || cmd.add != "" || cmd.alter != "" {
		t.Errorf("wrong cmd: %+v", cmd)
	}
	if cmd.coltype != nil {
		t.Errorf("wrong coltype: %+v", cmd)
	}
	if len(cmd.options) != 0 {
		t.Errorf("invalid options: %+v", cmd)
	}

	if err := parse("ALTER TABLE test WITH comment = 'comment'"); err != nil {
		t.Fatal(err)
	}
	if cmd.drop != "" || cmd.add != "" || cmd.alter != "" {
		t.Errorf("wrong cmd: %+v", cmd)
	}
	if cmd.coltype != nil {
		t.Errorf("wrong coltype: %+v", cmd)
	}
	if len(cmd.options) != 1 || cmd.options["comment"] == "" {
		t.Errorf("invalid options: %+v", cmd)
	}
}

func TestParseInsert(t *testing.T) {
	var cmd insertCommand
	parse := func(s string) error { return parseInto(s, &cmd) }

	if err := parse("INSERT INTO sometable (w) VALUES (?)"); err != nil {
		t.Fatal(err)
	}
	if cmd.table != "sometable" {
		t.Errorf("wrong table: %+v", cmd)
	}
	if !reflect.DeepEqual([]string{"w"}, cmd.keys) {
		t.Errorf("keys don't match: %+v", cmd)
	}
	if !reflect.DeepEqual([]pval{pval{VarIndex: 0}}, cmd.values) {
		t.Errorf("values don't match: %+v", cmd)
	}
	if cmd.cas {
		t.Errorf("cas should be false: %+v", cmd)
	}

	err := parse("INSERT INTO test (w, x, y, z) VALUES (1, ?, '3', ?) IF NOT EXISTS")
	if err != nil {
		t.Fatal(err)
	}
	if len(cmd.keys) != 4 || len(cmd.values) != 4 {
		t.Errorf("should be 4 keys and 4 values: %+v", cmd)
	}
	if !cmd.cas {
		t.Errorf("cas should be true: %+v", cmd)
	}
}

func TestParseUpdate(t *testing.T) {
	var cmd updateCommand
	parse := func(s string) error { return parseInto(s, &cmd) }

	if err := parse("UPDATE t SET x = 1 WHERE y = 2"); err != nil {
		t.Fatal(err)
	}
	if cmd.table != "t" {
		t.Errorf("wrong table: %+v", cmd)
	}
	if len(cmd.set) != 1 || cmd.set["x"].Value == nil {
		t.Errorf("wrong set: %+v", cmd)
	}
	if len(cmd.key) != 1 || cmd.key["y"].Value == nil {
		t.Errorf("wrong set: %+v", cmd)
	}

	if err := parse("UPDATE t SET x = ?, z = 'z' WHERE w = ? AND y = 2"); err != nil {
		t.Fatal(err)
	}
	if len(cmd.set) != 2 || cmd.set["x"].VarIndex != 0 || cmd.set["z"].Value == nil {
		t.Errorf("wrong set: %+v", cmd)
	}
	if len(cmd.key) != 2 || cmd.key["w"].VarIndex != 1 || cmd.key["y"].Value == nil {
		t.Errorf("wrong key: %+v", cmd)
	}
}

func TestParseSelect(t *testing.T) {
	var cmd selectCommand
	parse := func(s string) error { return parseInto(s, &cmd) }

	if err := parse("SELECT * FROM t"); err != nil {
		t.Fatal(err)
	}
	if cmd.table != "t" {
		t.Errorf("wrong table: %+v", cmd)
	}
	if len(cmd.cols) != 1 || cmd.cols[0] != "*" {
		t.Errorf("wrong cols: %+v", cmd)
	}
	if len(cmd.where) != 0 {
		t.Errorf("wrong where: %+v", cmd)
	}
	if len(cmd.order) != 0 {
		t.Errorf("wrong order: %+v", cmd)
	}
	if cmd.limit != 0 {
		t.Errorf("wrong limit: %+v", cmd)
	}

	if err := parse("SELECT COUNT(1) FROM t WHERE x >= ? LIMIT 1"); err != nil {
		t.Fatal(err)
	}
	if cmd.table != "t" {
		t.Errorf("wrong table: %+v", cmd)
	}
	if len(cmd.cols) != 1 || cmd.cols[0] != "count(*)" {
		t.Errorf("wrong cols: %+v", cmd)
	}
	if len(cmd.where) != 1 || cmd.where[0].col != "x" || cmd.where[0].op != ">=" {
		t.Errorf("wrong where: %+v", cmd)
	}
	if len(cmd.order) != 0 {
		t.Errorf("wrong order: %+v", cmd)
	}
	if cmd.limit != 1 {
		t.Errorf("wrong limit: %+v", cmd)
	}

	if err := parse("SELECT x, y, z FROM t WHERE x = ? AND y > 0 ORDER BY z"); err != nil {
		t.Fatal(err)
	}
	if cmd.table != "t" {
		t.Errorf("wrong table: %+v", cmd)
	}
	if len(cmd.cols) != 3 || cmd.cols[0] != "x" || cmd.cols[1] != "y" || cmd.cols[2] != "z" {
		t.Errorf("wrong cols: %+v", cmd)
	}
	if len(cmd.where) != 2 || cmd.where[0].col != "x" || cmd.where[1].col != "y" {
		t.Errorf("wrong where: %+v", cmd)
	}
	if len(cmd.order) != 1 || cmd.order[0].col != "z" || cmd.order[0].dir != asc {
		t.Errorf("wrong order: %+v", cmd)
	}
	if cmd.limit != 0 {
		t.Errorf("wrong limit: %+v", cmd)
	}

	if err := parse("SELECT * FROM t ORDER BY x DESC, y LIMIT 3"); err != nil {
		t.Fatal(err)
	}
	if cmd.table != "t" {
		t.Errorf("wrong table: %+v", cmd)
	}
	if len(cmd.order) != 2 || cmd.order[0].col != "x" || cmd.order[0].dir != desc {
		t.Errorf("wrong order: %+v", cmd)
	}
	if cmd.order[1].col != "y" || cmd.order[1].dir != asc {
		t.Errorf("wrong order: %+v", cmd)
	}
	if cmd.limit != 3 {
		t.Errorf("wrong limit: %+v", cmd)
	}
}
