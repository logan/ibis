package ibis

import "errors"
import "fmt"
import "reflect"
import "testing"

import "tux21b.org/v1/gocql"

import . "github.com/smartystreets/goconvey/convey"

func parseInto(s string, dest interface{}) pToken {
	tok := parseWith(s, pStatement)
	if tok.err != nil {
		return tok
	}
	destval := reflect.ValueOf(dest)
	if destval.Type().Kind() != reflect.Ptr {
		tok.err = errors.New("parseInto dest requires a pointer")
		return tok
	}
	elem := destval.Elem()
	ctx := reflect.ValueOf(tok.ctx).Elem()
	if elem.Type() != ctx.Type() {
		tok.err = errors.New(fmt.Sprintf("dest type %s incompatible with ctx type %s",
			elem.Type(), ctx.Type()))
		return tok
	}
	elem.Set(ctx)
	return tok
}

func shouldFailNear(actual interface{}, expected ...interface{}) string {
	tok := actual.(pToken)
	if tok.err == nil {
		return "parse should have failed but did not"
	}
	return ShouldStartWith(string(tok.runes), expected...)
}

func shouldParse(actual interface{}, expected ...interface{}) string {
	tok := actual.(pToken)
	return ShouldBeNil(tok.err)
}

func TestCompile(t *testing.T) {
	Convey("Statement should compile", t, func() {
		s := newStatement("SELECT * FROM t")
		So(s.Compile(), ShouldBeNil)
		So(s.cmd, ShouldHaveSameTypeAs, &selectCommand{})
	})

	Convey("Parse error should point to location", t, func() {
		s := newStatement("SELECT FROM")
		err := s.Compile()
		So(err, ShouldNotBeNil)
		So(err.Error(), ShouldContainSubstring, "\n       ^")
	})
}

func TestPToken(t *testing.T) {
	Convey("minus should safely return substring", t, func() {
		t := pToken{runes: []rune("test")}
		u := t.advance(3)
		So(u.minus(t), ShouldEqual, "tes")
		So(t.minus(u), ShouldEqual, "")
	})

	Convey("advance should never advance past end of runes", t, func() {
		t := pToken{runes: []rune("test")}
		u := t.advance(3)
		So(string(u.runes), ShouldEqual, "t")
		u = u.advance(3)
		So(string(u.runes), ShouldEqual, "")
	})
}

func TestGeneralParseErrors(t *testing.T) {
	var cmd insertCommand
	parse := func(s string) pToken { return parseInto(s, &cmd) }

	Convey("Unrecognized symbols should cause error", t, func() {
		So(parse("INSERT!"), shouldFailNear, "")
	})

	Convey("Unrecognized command should cause error", t, func() {
		So(parse("insret"), shouldFailNear, "insret")
		So(parse("VARCHAR"), shouldFailNear, "VARCHAR")
	})
}

func TestParseValue(t *testing.T) {
	var cmd insertCommand
	parse := func(s string) pToken { return parseInto(s, &cmd) }

	Convey("Placeholders should produce incrementing VarIndex", t, func() {
		So(parse("INSERT INTO t (x, y, z) VALUES (?, ?, ?)"), shouldParse)
		So(cmd.values, ShouldResemble,
			[]pval{pval{VarIndex: 0}, pval{VarIndex: 1}, pval{VarIndex: 2}})
	})

	Convey("Integer literals", t, func() {
		So(parse("INSERT INTO t (x, y, z) VALUES (1, 123, 8421)"), shouldParse)
		expected := []pval{
			pval{Value: LiteralValue(1)},
			pval{Value: LiteralValue(123)},
			pval{Value: LiteralValue(8421)},
		}
		So(cmd.values, ShouldResemble, expected)
		So(parse("INSERT INTO t (x) VALUES (999999999999999999999999999999999999999)"),
			shouldFailNear, "999999999999999999999999999999999999999)")
	})

	Convey("String literals", t, func() {
		So(parse("INSERT INTO t (x) VALUES ('abc')"), shouldParse)
		expected := []pval{pval{Value: LiteralValue("abc")}}
		So(cmd.values, ShouldResemble, expected)

		So(parse(`INSERT INTO t (x) VALUES ('ab\'c\\de\f')`), shouldParse)
		expected[0].Value = LiteralValue(`ab'c\def`)
		So(cmd.values, ShouldResemble, expected)

		So(parse("INSERT INTO t (x) VALUES ('abc"), shouldFailNear, "'abc")
		So(parse(`INSERT INTO t (x) VALUES ('abc\')`), shouldFailNear, `'abc\'`)
	})

	Convey("Map and set literals", t, func() {
		So(parse("INSERT INTO t (x) VALUES ({?, ?, ?})"), shouldParse)
		expected := []pval{pval{Value: LiteralValue("")}}
		So(cmd.values, ShouldResemble, expected)

		So(parse("INSERT INTO t (x) VALUES ({?: ?, ?: ?, ?: ?})"), shouldParse)
		So(cmd.values, ShouldResemble, expected)

		So(parse("INSERT INTO t (x) VALUES ({"), shouldFailNear, "")
		So(parse("INSERT INTO t (x) VALUES ({?,})"), shouldFailNear, "})")
		So(parse("INSERT INTO t (x) VALUES ({?,?:?})"), shouldFailNear, ":?")
		So(parse("INSERT INTO t (x) VALUES ({?,x})"), shouldFailNear, "x})")
		So(parse("INSERT INTO t (x) VALUES ({?:})"), shouldFailNear, "})")
		So(parse("INSERT INTO t (x) VALUES ({?:?,})"), shouldFailNear, "})")
		So(parse("INSERT INTO t (x) VALUES ({?:?,?})"), shouldFailNear, "})")
	})

	Convey("List literals", t, func() {
		So(parse("INSERT INTO t (x) VALUES ([?])"), shouldParse)
		expected := []pval{pval{Value: LiteralValue("")}}
		So(cmd.values, ShouldResemble, expected)

		So(parse("INSERT INTO t (x) VALUES ([?, ?, ?])"), shouldParse)
		So(cmd.values, ShouldResemble, expected)
	})
}

func TestParseCreateKeyspace(t *testing.T) {
	var cmd createKeyspaceCommand
	parse := func(s string) pToken { return parseInto(s, &cmd) }

	Convey("Parse errors should be caught", t, func() {
		So(parse("CREATE keespace test"), shouldFailNear, "keespace")
		So(parse("CREATE select test"), shouldFailNear, "select")
		So(parse("CREATE KEYSPACE"), shouldFailNear, "")
		So(parse("CREATE KEYSPACE 123"), shouldFailNear, "123")
		So(parse("CREATE KEYSPACE test garbage"), shouldFailNear, "garbage")
		So(parse("CREATE KEYSPACE test WITH"), shouldFailNear, "")
		So(parse("CREATE KEYSPACE test WITH 123"), shouldFailNear, "123")
		So(parse("CREATE KEYSPACE test WITH x 123"), shouldFailNear, "123")
		So(parse("CREATE KEYSPACE test WITH x = 123 AND"), shouldFailNear, "")
		So(parse("CREATE KEYSPACE test WITH x = 123 AND y = 'test' xxx"), shouldFailNear, "xxx")
	})

	Convey("CREATE KEYSPACE <table> should parse correctly", t, func() {
		So(parse("CREATE KEYSPACE test"), shouldParse)
		So(cmd.identifier, ShouldEqual, "test")
		So(cmd.strict, ShouldBeTrue)
		So(cmd.options, ShouldBeNil)
	})

	Convey("CREATE KEYSPACE IF NOT EXISTS <table> should parse correctly", t, func() {
		So(parse("CREATE KEYSPACE IF NOT EXISTS test"), shouldParse)
		So(cmd.identifier, ShouldEqual, "test")
		So(cmd.strict, ShouldBeFalse)
	})

	Convey("Options should parse correctly", t, func() {
		So(parse("CREATE KEYSPACE IF NOT EXISTS test WITH x=1"), shouldParse)
		expected := optionMap{"x": pval{Value: LiteralValue(1)}}
		So(cmd.options, ShouldResemble, expected)

		So(parse("CREATE KEYSPACE test WITH x=1 AND y = 'str'"), shouldParse)
		expected["y"] = pval{Value: LiteralValue("str")}
		So(cmd.options, ShouldResemble, expected)
	})
}

func TestParseCreateTable(t *testing.T) {
	var cmd createTableCommand
	parse := func(s string) pToken { return parseInto(s, &cmd) }

	Convey("Parse errors should be caught", t, func() {
		So(parse("CREATE TABLE"), shouldFailNear, "")
		So(parse("CREATE TABLE table"), shouldFailNear, "table")
		So(parse("CREATE TABLE IF ("), shouldFailNear, "IF (")
		So(parse("CREATE TABLE IF EXISTS"), shouldFailNear, "IF EXISTS")
		So(parse("CREATE TABLE IF NOT EXISTS"), shouldFailNear, "")
		So(parse("CREATE TABLE test"), shouldFailNear, "")
		So(parse("CREATE TABLE test x"), shouldFailNear, "x")
		So(parse("CREATE TABLE test (x"), shouldFailNear, "")
		So(parse("CREATE TABLE test (x,"), shouldFailNear, ",")
		So(parse("CREATE TABLE test (x varchar"), shouldFailNear, "")
		So(parse("CREATE TABLE test (x varchar) garbage"), shouldFailNear, "garbage")
		So(parse("CREATE TABLE test (x varchar primary,)"), shouldFailNear, ",")
		So(parse("CREATE TABLE test (x varchar primary key,)"), shouldFailNear, ")")
		So(parse("CREATE TABLE test (x blob, primary (x))"), shouldFailNear, "(x)")
		So(parse("CREATE TABLE test (x blob, primary key)"), shouldFailNear, ")")
		So(parse("CREATE TABLE test (x blob, primary key ()"), shouldFailNear, ")")
		So(parse("CREATE TABLE test (x blob, primary key (x"), shouldFailNear, "")
		So(parse("CREATE TABLE test (x blob, primary key (x,)"), shouldFailNear, ")")
		So(parse("CREATE TABLE test (x blob, primary key (x,,"), shouldFailNear, ",")
		So(parse("CREATE TABLE test (x blob, primary key (x)"), shouldFailNear, "")
		So(parse("CREATE TABLE test (x varchar) WITH"), shouldFailNear, "")
		So(parse("CREATE TABLE test (x varchar) WITH x ="), shouldFailNear, "")
		So(parse("CREATE TABLE test (x varchar) WITH x 123"), shouldFailNear, "123")
		So(parse("CREATE TABLE test (x blob, primary key (x)) garbage"), shouldFailNear, "garbage")
		So(parse("CREATE TABLE test (x timeuuid) WITH x = 123 garbage"), shouldFailNear, "garbage")
	})

	Convey("Simple table", t, func() {
		So(parse("CREATE TABLE t (x varchar)"), shouldParse)
		So(cmd.identifier, ShouldEqual, "t")
		So(cmd.colnames, ShouldResemble, []string{"x"})
		So(cmd.coltypes, ShouldResemble, []*gocql.TypeInfo{TIVarchar})
		So(cmd.strict, ShouldBeTrue)
	})

	Convey("COLUMNFAMILY should be an acceptable alternative for TABLE", t, func() {
		So(parse("CREATE COLUMNFAMILY t (x varchar)"), shouldParse)
		So(cmd.identifier, ShouldEqual, "t")
	})

	Convey("Primary keys", t, func() {
		So(parse("CREATE TABLE t (x blob PRIMARY KEY)"), shouldParse)
		So(cmd.key, ShouldResemble, []string{"x"})

		So(parse("CREATE TABLE t (x blob, PRIMARY KEY (x, y), y blob)"), shouldParse)
		So(cmd.key, ShouldResemble, []string{"x", "y"})
	})

	Convey("Multiple primary key definitions should be caught", t, func() {
		So(parse("CREATE TABLE t (x blob primary key, primary key(x)) "), shouldFailNear, ") ")
		So(parse("CREATE TABLE t (x blob primary key, y blob primary key)"),
			shouldFailNear, ")")
		So(parse("CREATE TABLE t (x blob, primary key(x), primary key (x)) "),
			shouldFailNear, ") ")
	})

	Convey("IF NOT EXISTS", t, func() {
		So(parse("CREATE TABLE IF NOT EXISTS t (x blob PRIMARY KEY)"), shouldParse)
		So(cmd.strict, ShouldBeFalse)
	})

	Convey("Options should parse correctly", t, func() {
		So(parse("CREATE TABLE test (x varchar) WITH x=1"), shouldParse)
		expected := optionMap{"x": pval{Value: LiteralValue(1)}}
		So(cmd.options, ShouldResemble, expected)

		So(parse("CREATE TABLE test (x varchar) WITH x=1 AND y = 'str'"), shouldParse)
		expected["y"] = pval{Value: LiteralValue("str")}
		So(cmd.options, ShouldResemble, expected)
	})
}

func TestParseDrop(t *testing.T) {
	var cmd dropCommand
	parse := func(s string) pToken { return parseInto(s, &cmd) }

	Convey("Errors should be caught", t, func() {
		So(parse("DROP"), shouldFailNear, "")
		So(parse("DROP IF"), shouldFailNear, "IF")
		So(parse("DROP test"), shouldFailNear, "test")
		So(parse("DROP TABLE"), shouldFailNear, "")
		So(parse("DROP TABLE KEYSPACE"), shouldFailNear, "KEYSPACE")
		So(parse("DROP KEYSPACE IF"), shouldFailNear, "")
		So(parse("DROP KEYSPACE IF x"), shouldFailNear, "")
		So(parse("DROP KEYSPACE IF EXISTS 123"), shouldFailNear, "123")
		So(parse("DROP KEYSPACE x IF EXISTS"), shouldFailNear, "IF EXISTS")
	})

	Convey("Basic DROP", t, func() {
		So(parse("DROP KEYSPACE x"), shouldParse)
		So(cmd.dropType, ShouldEqual, "keyspace")
		So(cmd.identifier, ShouldEqual, "x")
		So(cmd.strict, ShouldBeTrue)

		So(parse("DROP TABLE x"), shouldParse)
		So(cmd.dropType, ShouldEqual, "table")
		So(cmd.identifier, ShouldEqual, "x")
		So(cmd.strict, ShouldBeTrue)
	})

	Convey("IF EXISTS", t, func() {
		So(parse("DROP KEYSPACE IF EXISTS x"), shouldParse)
		So(cmd.dropType, ShouldEqual, "keyspace")
		So(cmd.identifier, ShouldEqual, "x")
		So(cmd.strict, ShouldBeFalse)

		So(parse("DROP TABLE IF EXISTS x"), shouldParse)
		So(cmd.dropType, ShouldEqual, "table")
		So(cmd.identifier, ShouldEqual, "x")
		So(cmd.strict, ShouldBeFalse)
	})
}

func TestParseAlter(t *testing.T) {
	var cmd alterCommand
	parse := func(s string) pToken { return parseInto(s, &cmd) }

	Convey("Change type", t, func() {
		So(parse("ALTER TABLE test ALTER x TYPE varchar"), shouldParse)
		So(cmd.table, ShouldEqual, "test")
		So(cmd.alter, ShouldEqual, "x")
		So(cmd.add, ShouldEqual, "")
		So(cmd.drop, ShouldEqual, "")
		So(cmd.options, ShouldBeNil)
	})

	Convey("Add column", t, func() {
		So(parse("ALTER TABLE test ADD x varchar"), shouldParse)
		So(cmd.table, ShouldEqual, "test")
		So(cmd.add, ShouldEqual, "x")
		So(cmd.alter, ShouldEqual, "")
		So(cmd.drop, ShouldEqual, "")
		So(cmd.options, ShouldBeNil)
	})

	Convey("Drop column", t, func() {
		So(parse("ALTER TABLE test DROP x"), shouldParse)
		So(cmd.table, ShouldEqual, "test")
		So(cmd.drop, ShouldEqual, "x")
		So(cmd.add, ShouldEqual, "")
		So(cmd.alter, ShouldEqual, "")
		So(cmd.options, ShouldBeNil)
	})

	Convey("With options", t, func() {
		So(parse("ALTER TABLE test WITH comment = 'comment'"), shouldParse)
		expected := optionMap{"comment": pval{Value: LiteralValue("comment")}}
		So(cmd.options, ShouldResemble, expected)

		So(parse("ALTER TABLE test WITH comment = 'comment' AND x = 1"), shouldParse)
		expected["x"] = pval{Value: LiteralValue(1)}
		So(cmd.options, ShouldResemble, expected)
	})

	Convey("Parse errors should be caught", t, func() {
		So(parse("ALTER test"), shouldFailNear, "test")
		So(parse("ALTER ADD test"), shouldFailNear, "ADD")
		So(parse("ALTER TABLE"), shouldFailNear, "")
		So(parse("ALTER TABLE ADD"), shouldFailNear, "ADD")
		So(parse("ALTER TABLE test"), shouldFailNear, "")
		So(parse("ALTER TABLE test CREATE"), shouldFailNear, "CREATE")
		So(parse("ALTER TABLE test ADD"), shouldFailNear, "")
		So(parse("ALTER TABLE test ADD TYPE"), shouldFailNear, "TYPE")
		So(parse("ALTER TABLE test ADD x"), shouldFailNear, "")
		So(parse("ALTER TABLE test ADD x y"), shouldFailNear, "y")
		So(parse("ALTER TABLE test ADD x TYPE varchar"), shouldFailNear, "TYPE")
		So(parse("ALTER TABLE test ALTER x varchar"), shouldFailNear, "varchar")
		So(parse("ALTER TABLE test ALTER x TYPE y"), shouldFailNear, "y")
		So(parse("ALTER TABLE test DROP"), shouldFailNear, "")
		So(parse("ALTER TABLE test DROP TYPE"), shouldFailNear, "TYPE")
		So(parse("ALTER TABLE test WITH x 123"), shouldFailNear, "123")
		So(parse("ALTER TABLE test DROP x garbage"), shouldFailNear, "garbage")
	})
}

func TestParseInsert(t *testing.T) {
	var cmd insertCommand
	parse := func(s string) pToken { return parseInto(s, &cmd) }

	Convey("Inserting a single column value", t, func() {
		So(parse("INSERT INTO sometable (w) VALUES (?)"), shouldParse)
		So(cmd.table, ShouldEqual, "sometable")
		So(cmd.keys, ShouldResemble, []string{"w"})
		So(cmd.values, ShouldResemble, []pval{pval{VarIndex: 0}})
		So(cmd.cas, ShouldBeFalse)
	})

	Convey("Inserting multiple column values", t, func() {
		So(parse("INSERT INTO test (w, x, y, z) VALUES (1, ?, '3', ?)"), shouldParse)
		So(cmd.table, ShouldEqual, "test")
		So(cmd.keys, ShouldResemble, []string{"w", "x", "y", "z"})
		expectedValues := []pval{
			pval{Value: LiteralValue(1)},
			pval{VarIndex: 0},
			pval{Value: LiteralValue("3")},
			pval{VarIndex: 1},
		}
		So(cmd.values, ShouldResemble, expectedValues)
		So(cmd.cas, ShouldBeFalse)
	})

	Convey("IF NOT EXISTS should set strict to true", t, func() {
		So(parse("INSERT INTO test (w) VALUES (?) IF NOT EXISTS"), shouldParse)
		So(cmd.cas, ShouldBeTrue)
	})

	Convey("Parse errors should be caught", t, func() {
		So(parse("INSERT test"), shouldFailNear, "test")
		So(parse("INSERT VALUES"), shouldFailNear, "VALUES")
		So(parse("INSERT INTO VALUES"), shouldFailNear, "VALUES")
		So(parse("INSERT INTO ("), shouldFailNear, "(")
		So(parse("INSERT INTO test"), shouldFailNear, "")
		So(parse("INSERT INTO test VALUES"), shouldFailNear, "VALUES")
		So(parse("INSERT INTO test ()"), shouldFailNear, ")")
		So(parse("INSERT INTO test (x)"), shouldFailNear, "")
		So(parse("INSERT INTO test (x,)"), shouldFailNear, ")")
		So(parse("INSERT INTO test (x y"), shouldFailNear, "y")
		So(parse("INSERT INTO test (x) ("), shouldFailNear, "(")
		So(parse("INSERT INTO test (x) VALUES"), shouldFailNear, "")
		So(parse("INSERT INTO test (x) VALUES ?"), shouldFailNear, "?")
		So(parse("INSERT INTO test (x) VALUES (?"), shouldFailNear, "")
		So(parse("INSERT INTO test (x) VALUES (?,)"), shouldFailNear, ")")
		So(parse("INSERT INTO test (x) VALUES (?) garbage"), shouldFailNear, "garbage")
		So(parse("INSERT INTO test (x) VALUES (?) IF"), shouldFailNear, "")
		So(parse("INSERT INTO test (x) VALUES (?) IF EXISTS"), shouldFailNear, "IF EXISTS")
		So(parse("INSERT INTO test (x) VALUES (?) IF NOT"), shouldFailNear, "IF NOT")
		So(parse("INSERT INTO test (x) VALUES (?) IF NOT x"), shouldFailNear, "IF NOT x")
		So(parse("INSERT INTO test (x) VALUES (?) IF NOT EXISTS zzz"), shouldFailNear, "zzz")
	})
}

func TestParseUpdate(t *testing.T) {
	var cmd updateCommand
	parse := func(s string) pToken { return parseInto(s, &cmd) }

	Convey("Updating a single column", t, func() {
		So(parse("UPDATE t SET x = 1 WHERE y = 2"), shouldParse)
		So(cmd.table, ShouldEqual, "t")
		So(cmd.set, ShouldResemble, map[string]pval{"x": pval{Value: LiteralValue(1)}})
		So(cmd.key, ShouldResemble, map[string]pval{"y": pval{Value: LiteralValue(2)}})
	})

	Convey("Updating multiple columns", t, func() {
		So(parse("UPDATE t SET x = ?, z = 'z' WHERE w = ? AND y = 2"), shouldParse)
		expectedSet := map[string]pval{
			"x": pval{VarIndex: 0},
			"z": pval{Value: LiteralValue("z")},
		}
		So(cmd.set, ShouldResemble, expectedSet)
		expectedSet = map[string]pval{
			"w": pval{VarIndex: 1},
			"y": pval{Value: LiteralValue(2)},
		}
		So(cmd.key, ShouldResemble, expectedSet)
	})

	Convey("Parse errors should be caught", t, func() {
		So(parse("UPDATE"), shouldFailNear, "")
		So(parse("UPDATE SET"), shouldFailNear, "SET")
		So(parse("UPDATE t x"), shouldFailNear, "x")
		So(parse("UPDATE t WHERE"), shouldFailNear, "WHERE")
		So(parse("UPDATE t SET"), shouldFailNear, "")
		So(parse("UPDATE t SET WHERE"), shouldFailNear, "WHERE")
		So(parse("UPDATE t SET x ?"), shouldFailNear, "?")
		So(parse("UPDATE t SET x = ?"), shouldFailNear, "")
		So(parse("UPDATE t SET x = "), shouldFailNear, "")
		So(parse("UPDATE t SET x = WHERE"), shouldFailNear, "WHERE")
		So(parse("UPDATE t SET x = ?, WHERE"), shouldFailNear, "WHERE")
		So(parse("UPDATE t SET x = ? AND"), shouldFailNear, "AND")
		So(parse("UPDATE t SET x = ? WHERE"), shouldFailNear, "")
		So(parse("UPDATE t SET x = ? WHERE y"), shouldFailNear, "")
		So(parse("UPDATE t SET x = ? WHERE y ="), shouldFailNear, "")
		So(parse("UPDATE t SET x = ? WHERE y = AND"), shouldFailNear, "AND")
		So(parse("UPDATE t SET x = ? WHERE y = ?,"), shouldFailNear, ",")
		So(parse("UPDATE t SET x = ? WHERE y = ? AND"), shouldFailNear, "")
		So(parse("UPDATE t SET x = ? WHERE y = ? garbage"), shouldFailNear, "garbage")
	})
}

func TestParseDelete(t *testing.T) {
	var cmd deleteCommand
	parse := func(s string) pToken { return parseInto(s, &cmd) }

	Convey("Valid commands should parse", t, func() {
		So(parse("DELETE FROM t WHERE x = 1"), shouldParse)
		So(cmd.table, ShouldEqual, "t")
		expected := map[string]pval{"x": pval{Value: LiteralValue(1)}}
		So(cmd.key, ShouldResemble, expected)

		So(parse("DELETE FROM t WHERE x = 1 AND y = 2"), shouldParse)
		So(cmd.table, ShouldEqual, "t")
		expected["y"] = pval{Value: LiteralValue(2)}
		So(cmd.key, ShouldResemble, expected)
	})

	Convey("Parse errors should be caught", t, func() {
		So(parse("DELETE t"), shouldFailNear, "t")
		So(parse("DELETE WHERE"), shouldFailNear, "WHERE")
		So(parse("DELETE FROM"), shouldFailNear, "")
		So(parse("DELETE FROM WHERE"), shouldFailNear, "WHERE")
		So(parse("DELETE FROM t"), shouldFailNear, "")
		So(parse("DELETE FROM t IF"), shouldFailNear, "IF")
		So(parse("DELETE FROM t x"), shouldFailNear, "x")
		So(parse("DELETE FROM t WHERE"), shouldFailNear, "")
		So(parse("DELETE FROM t WHERE x"), shouldFailNear, "")
		So(parse("DELETE FROM t WHERE x ?"), shouldFailNear, "?")
		So(parse("DELETE FROM t WHERE x ="), shouldFailNear, "")
		So(parse("DELETE FROM t WHERE x = ? AND"), shouldFailNear, "")
		So(parse("DELETE FROM t WHERE x = ? AND y"), shouldFailNear, "")
		So(parse("DELETE FROM t WHERE x = ? garbage"), shouldFailNear, "garbage")
	})
}

func TestParseSelect(t *testing.T) {
	var cmd selectCommand
	parse := func(s string) pToken { return parseInto(s, &cmd) }

	Convey("Selection columns", t, func() {
		So(parse("SELECT x FROM t"), shouldParse)
		So(cmd.table, ShouldEqual, "t")
		So(cmd.cols, ShouldResemble, []string{"x"})
		/*
		   So(cmd.where, ShouldResemble, []comparison{})
		   So(cmd.order, ShouldResemble, []order{})
		*/
		So(len(cmd.where), ShouldEqual, 0)
		So(len(cmd.order), ShouldEqual, 0)
		So(cmd.limit, ShouldEqual, 0)

		So(parse("SELECT x, y, z FROM t"), shouldParse)
		So(cmd.cols, ShouldResemble, []string{"x", "y", "z"})

		So(parse("SELECT * FROM t"), shouldParse)
		So(cmd.cols, ShouldResemble, []string{"*"})

		So(parse("SELECT COUNT(*) FROM t"), shouldParse)
		So(cmd.cols, ShouldResemble, []string{"count(*)"})

		So(parse("SELECT COUNT(1) FROM t"), shouldParse)
		So(cmd.cols, ShouldResemble, []string{"count(*)"})
	})

	Convey("Where conditions", t, func() {
		So(parse("SELECT * FROM t WHERE x >= ?"), shouldParse)
		expected := make([]comparison, 0)
		expected = append(expected, comparison{"x", ">=", pval{VarIndex: 0}})
		So(cmd.where, ShouldResemble, expected)

		So(parse("SELECT * FROM t WHERE x >= ? AND y = 1 AND z < ?"), shouldParse)
		expected = append(expected,
			comparison{"y", "=", pval{Value: LiteralValue(1)}},
			comparison{"z", "<", pval{VarIndex: 1}})
		So(cmd.where, ShouldResemble, expected)
	})

	Convey("Orderings", t, func() {
		So(parse("SELECT * FROM t ORDER BY x"), shouldParse)
		expected := make([]order, 0)
		expected = append(expected, order{"x", asc})
		So(cmd.order, ShouldResemble, expected)

		So(parse("SELECT * FROM t WHERE x = ? ORDER BY x LIMIT 1"), shouldParse)
		So(cmd.where, ShouldResemble, []comparison{comparison{"x", "=", pval{VarIndex: 0}}})
		So(cmd.order, ShouldResemble, expected)

		So(parse("SELECT * FROM t ORDER BY x, y DESC, z ASC"), shouldParse)
		expected = append(expected, order{"y", desc}, order{"z", asc})
		So(cmd.order, ShouldResemble, expected)
	})

	Convey("Limit", t, func() {
		So(parse("SELECT * FROM t LIMIT 1"), shouldParse)
		So(cmd.limit, ShouldEqual, 1)

		So(parse("SELECT * FROM t WHERE x = ? LIMIT 1"), shouldParse)
		So(cmd.where, ShouldResemble, []comparison{comparison{"x", "=", pval{VarIndex: 0}}})
		So(cmd.limit, ShouldEqual, 1)

		So(parse("SELECT * FROM t ORDER BY x DESC LIMIT 1"), shouldParse)
		So(cmd.order, ShouldResemble, []order{order{"x", desc}})
		So(cmd.limit, ShouldEqual, 1)

		So(parse("SELECT * FROM t WHERE x = ? ORDER BY x DESC LIMIT 8421"), shouldParse)
		So(cmd.where, ShouldResemble, []comparison{comparison{"x", "=", pval{VarIndex: 0}}})
		So(cmd.order, ShouldResemble, []order{order{"x", desc}})
		So(cmd.limit, ShouldEqual, 8421)
	})

	Convey("Parse errors should be caught", t, func() {
		So(parse("SELECT COUNT"), shouldFailNear, "")
		So(parse("SELECT COUNT *"), shouldFailNear, "*")
		So(parse("SELECT COUNT("), shouldFailNear, "")
		So(parse("SELECT COUNT()"), shouldFailNear, ")")
		So(parse("SELECT COUNT(x)"), shouldFailNear, "x")
		So(parse("SELECT COUNT(*"), shouldFailNear, "")
		So(parse("SELECT COUNT(123"), shouldFailNear, "")
		So(parse("SELECT FROM"), shouldFailNear, "FROM")
		So(parse("SELECT * t"), shouldFailNear, "t")
		So(parse("SELECT *,"), shouldFailNear, ",")
		So(parse("SELECT count(*),"), shouldFailNear, ",")
		So(parse("SELECT x, FROM"), shouldFailNear, "FROM")
		So(parse("SELECT x, *"), shouldFailNear, "*")
		So(parse("SELECT * FROM WHERE"), shouldFailNear, "WHERE")
		So(parse("SELECT * FROM t WHERE"), shouldFailNear, "")
		So(parse("SELECT * FROM t WHERE >"), shouldFailNear, ">")
		So(parse("SELECT * FROM t WHERE x"), shouldFailNear, "")
		So(parse("SELECT * FROM t WHERE x <>"), shouldFailNear, ">")
		So(parse("SELECT * FROM t WHERE x !="), shouldFailNear, "!=")
		So(parse("SELECT * FROM t WHERE x ,"), shouldFailNear, ",")
		So(parse("SELECT * FROM t WHERE x ="), shouldFailNear, "")
		So(parse("SELECT * FROM t WHERE x = ? AND"), shouldFailNear, "")
		So(parse("SELECT * FROM t WHERE x = ? AND ORDER BY"), shouldFailNear, "ORDER")
		So(parse("SELECT * FROM t WHERE x = ? ORDER x"), shouldFailNear, "x")
		So(parse("SELECT * FROM t WHERE x = ? ORDER DESC x"), shouldFailNear, "DESC")
		So(parse("SELECT * FROM t WHERE x = ? ORDER BY"), shouldFailNear, "")
		So(parse("SELECT * FROM t WHERE x = ? ORDER BY x,"), shouldFailNear, "")
		So(parse("SELECT * FROM t WHERE x = ? ORDER BY x, LIMIT"), shouldFailNear, "LIMIT")
		So(parse("SELECT * FROM t WHERE x = ? LIMIT"), shouldFailNear, "")
		So(parse("SELECT * FROM t WHERE x = ? LIMIT x"), shouldFailNear, "x")
		So(parse("SELECT * FROM t garbage"), shouldFailNear, "garbage")
		So(parse("SELECT * FROM t WHERE x = ? garbage"), shouldFailNear, "garbage")
		So(parse("SELECT * FROM t ORDER BY x garbage"), shouldFailNear, "garbage")
		So(parse("SELECT * FROM t ORDER BY x DESC garbage"), shouldFailNear, "garbage")
		So(parse("SELECT * FROM t WHERE x = ? LIMIT garbage"), shouldFailNear, "garbage")
	})
}
