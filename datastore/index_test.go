package datastore

import "fmt"
import "strconv"
import "testing"

type indexTester interface {
	setSeqIDInterval(string)
	tearDown()
	scanBySeqID() indexScanTester
	scanBy(...string) indexScanTester
	newRow() Row
	makeRow(string, int64, bool) (Row, error)
}

type indexScanTester interface {
	where(...interface{}) indexScanTester
	expect(string, int, ...Row) (string, bool)
}

type indexTestEnv struct {
	orm   *indexTestOrm
	seqid indexTestSeqIDGenerator
}

func newIndexTestEnv(t *testing.T) indexTester {
	env := &indexTestEnv{}
	env.orm = &indexTestOrm{}
	tc := NewTestConn(t)
	schema := ReflectSchemaFrom(&env.orm.M)
	orm := &Orm{CassandraConn: tc.CassandraConn, Model: schema, SeqID: &env.seqid}
	schema.Bind(orm)
	var err error
	if orm.SchemaUpdates, err = DiffLiveSchema(tc.CassandraConn, schema); err != nil {
		t.Fatal(err)
	}
	if err = orm.ApplySchemaUpdates(); err != nil {
		t.Fatal(err)
	}
	env.orm.Orm = orm
	env.orm.TestConn = tc
	return env
}

func (env *indexTestEnv) setSeqIDInterval(interval string) {
	s := intervalToSeqID(interval)
	i, _ := strconv.ParseUint(string(s), 36, 64)
	env.seqid = indexTestSeqIDGenerator(i)
}

func (env *indexTestEnv) tearDown() {
	env.orm.Orm.Close()
}

func (env *indexTestEnv) newRow() Row {
	return env.orm.M.T.NewRow()
}

func (env *indexTestEnv) makeRow(name string, number int64, status bool) (Row, error) {
	p := env.orm.M.T.NewP()
	p.Name = name
	p.Number = number
	p.Status = status
	return p, env.orm.M.T.CF().CommitCAS(p)
}

func (env *indexTestEnv) scanBySeqID() indexScanTester {
	return newIndexScanTest(env, env.orm.Orm.Model.IndexBySeqID(env.orm.M.T))
}

func (env *indexTestEnv) scanBy(cols ...string) indexScanTester {
	return newIndexScanTest(env, env.orm.Orm.Model.IndexBy(env.orm.M.T, cols...))
}

type indexTestOrm struct {
	*Orm
	*TestConn
	M struct {
		T *indexTestT
	}
}

// Define a seqid generator that we can control, to force particular intervals in the index.
type indexTestSeqIDGenerator uint64

func (g *indexTestSeqIDGenerator) New() (SeqID, error) {
	*g++
	return SeqID(strconv.FormatUint(uint64(*g), 36)), nil
}

func (g *indexTestSeqIDGenerator) CurrentInterval() string {
	return interval(SeqID(strconv.FormatUint(uint64(*g), 36)))
}

// Define a trivial model for testing a seqid index and a couple of compound indexes.
type indexTestP struct {
	SeqIDRow
	Name   string
	Number int64
	Status bool
}

func (p *indexTestP) String() string {
	return fmt.Sprintf("(%s:%s,%d,%v)", p.SeqID, p.Name, p.Number, p.Status)
}

type indexTestT ColumnFamily

func (t *indexTestT) ConfigureCF(options *CFOptions) {
	options.Key("Name")
	options.AddIndexBySeqID()
	options.AddIndexBy("Number")
	options.AddIndexBy("Number", "Status")
}

func (t *indexTestT) CF() *ColumnFamily {
	return (*ColumnFamily)(t)
}

func (t *indexTestT) NewRow() Row {
	p := &indexTestP{}
	p.CF = t.CF()
	return p.Reflect(p)
}

func (t *indexTestT) NewP() *indexTestP {
	return t.NewRow().(*indexTestP)
}

// Define convenience functions for iterating over one of our indexes.
type indexScanTest struct {
	env  indexTester
	iter *IndexIter
}

func newIndexScanTest(env indexTester, idx *Index) indexScanTester {
	return &indexScanTest{env, idx.Iter()}
}

func (scan *indexScanTest) where(w ...interface{}) indexScanTester {
	scan.iter.LowerBound = "0"
	scan.iter.By(w...)
	return scan
}

func (scan *indexScanTest) expect(after string, limit int, expected ...Row) (string, bool) {
	received := make([]Row, 0, len(expected))
	scan.iter.After = SeqID(after)
	scan.iter.Limit = limit
	for {
		row := scan.env.newRow()
		if !scan.iter.Next(row) {
			break
		}
		received = append(received, row)
	}
	if scan.iter.Err != nil {
		return fmt.Sprintf("unexpected error: %s", scan.iter.Err), false
	}

	msg := fmt.Sprintf("\nexpected: %+v\nreceived: %+v", expected, received)
	if len(received) != len(expected) {
		return msg, false
	}
	for i, exp := range expected {
		if !rowsEqual(exp, received[i]) {
			return msg, false
		}
	}
	return "", true
}

func TestSeqIDListing(t *testing.T) {
	env := newIndexTestEnv(t)
	defer env.tearDown()

	// try empty listing
	if msg, ok := env.scanBySeqID().expect("", 0); !ok {
		t.Fatal(msg)
	}

	// Insert first item and see if the index returns it.
	row1, err := env.makeRow("test1", 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if msg, ok := env.scanBySeqID().expect("", 0, row1); !ok {
		t.Fatal(msg)
	}

	// Insert a second item two intervals ahead of the first.
	env.setSeqIDInterval("2")
	row2, err := env.makeRow("test2", 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if msg, ok := env.scanBySeqID().expect("", 0, row2, row1); !ok {
		t.Fatal(msg)
	}

	// Test limit.
	if msg, ok := env.scanBySeqID().expect("", 1, row2); !ok {
		t.Fatal(msg)
	}

	// Test after.
	if msg, ok := env.scanBySeqID().expect("2", 0, row1); !ok {
		t.Fatal(msg)
	}
}

func TestCompoundSeqIDListing(t *testing.T) {
	env := newIndexTestEnv(t)
	defer env.tearDown()

	// Try empty listings
	if msg, ok := env.scanBy("Number").where(1001).expect("", 0); !ok {
		t.Fatal(msg)
	}
	if msg, ok := env.scanBy("Number", "Status").where(1001, true).expect("", 0); !ok {
		t.Fatal(msg)
	}

	// Make some test data
	makeR := func(name string, number int64, status bool) Row {
		row, err := env.makeRow(name, number, status)
		if err != nil {
			t.Fatal(err)
		}
		return row
	}
	r1 := makeR("test1", 1001, true)
	r2 := makeR("test2", 1002, false)
	r3 := makeR("test3", 1001, false)
	r4 := makeR("test4", 1002, true)
	r5 := makeR("test5", 1001, true)

	// Scan by each Number value.
	if msg, ok := env.scanBy("Number").where(1001).expect("", 0, r5, r3, r1); !ok {
		t.Fatal(msg)
	}
	if msg, ok := env.scanBy("Number", "Status").where(1001, true).expect("", 0, r5, r1); !ok {
		t.Fatal(msg)
	}
	if msg, ok := env.scanBy("Number").where(1002).expect("", 0, r4, r2); !ok {
		t.Fatal(msg)
	}
	if msg, ok := env.scanBy("Number", "Status").where(1002, false).expect("", 0, r2); !ok {
		t.Fatal(msg)
	}

	// Make sure after and limit work.
	if msg, ok := env.scanBy("Number").where(1001).expect("4", 1, r3); !ok {
		t.Fatal(msg)
	}
}
