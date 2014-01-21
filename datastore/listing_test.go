package datastore

import "fmt"
import "reflect"
import "strconv"
import "testing"

type testSeqIDGenerator uint64

func (g *testSeqIDGenerator) New() (SeqID, error) {
	*g++
	return SeqID(strconv.FormatUint(uint64(*g), 36)), nil
}

func (g *testSeqIDGenerator) CurrentInterval() string {
	return interval(SeqID(strconv.FormatUint(uint64(*g), 36)))
}

type seqidTestP struct {
	SeqIDPersistent
	Name string
}

type seqidTestT ColumnFamily

func (t *seqidTestT) ConfigureCF(options *CFOptions) {
	options.Key("Name")
	AddSeqIDIndex(options)
}

func (t *seqidTestT) CF() *ColumnFamily {
	return (*ColumnFamily)(t)
}

func (t *seqidTestT) NewRow() Row {
	return t.NewP()
}

func (t *seqidTestT) NewP() *seqidTestP {
	p := &seqidTestP{}
	p.CF = t.CF()
	return p
}

type seqidTestO struct {
	*Orm
	*TestConn
	tsg testSeqIDGenerator
	M   struct {
		T *seqidTestT
	}
}

type seqidTestIter struct {
	*SeqIDListingIter
}

func (i *seqidTestIter) Next(p *seqidTestP) bool {
	return i.SeqIDListingIter.Next(p)
}

func (i *seqidTestIter) Error() error {
	return i.SeqIDListingIter.Err
}

func seqidTestAll(t *seqidTestT, after string, limit int) ([]*seqidTestP, error) {
	items := make([]*seqidTestP, 0)
	i := &seqidTestIter{IterSeqIDListing(t)}
	i.After = SeqID(after)
	i.Limit = limit
	p := t.NewP()
	for i.Next(p) {
		items = append(items, p)
		p = t.NewP()
	}
	return items, i.Error()
}

func newSeqidTestO(t *testing.T) *seqidTestO {
	sto := &seqidTestO{}
	tc := NewTestConn(t)
	schema := ReflectSchemaFrom(&sto.M)
	orm := &Orm{CassandraConn: tc.CassandraConn, Model: schema, SeqID: &sto.tsg}
	schema.Bind(orm)
	var err error
	if orm.SchemaUpdates, err = DiffLiveSchema(tc.CassandraConn, schema); err != nil {
		t.Fatal(err)
	}
	if err = orm.ApplySchemaUpdates(); err != nil {
		t.Fatal(err)
	}
	sto.Orm = orm
	sto.TestConn = tc
	return sto
}

func (o *seqidTestO) Close() {
	o.Orm.Close()
}

func TestSeqIDListing(t *testing.T) {
	o := newSeqidTestO(t)
	defer o.Close()

	// try empty listing
	if items, err := seqidTestAll(o.M.T, "", 0); len(items) > 0 || err != nil {
		if err != nil {
			t.Fatal(err)
		} else {
			t.Fatal("should not have found any items")
		}
	}

	p := o.M.T.NewP()
	p.Name = "1"
	if err := o.M.T.CF().CommitCAS(p); err != nil {
		t.Fatal(err)
	}
	if p.SeqID == "" {
		t.Fatal("expected seqid to be filled in")
	}

	items, err := seqidTestAll(o.M.T, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatal(fmt.Sprintf("expected exactly one item, found %d (%+v)", len(items), items))
	}
	if !reflect.DeepEqual(p, items[0]) {
		t.Fatal(fmt.Sprintf("\nexpected: %+v\nreceived: %+v", p, items[0]))
	}

	// Insert a second item two intervals ahead of the first.
	oldi := interval(p.SeqID)
	newi := incrInterval(incrInterval(oldi))
	x, _ := strconv.ParseUint(string(intervalToSeqID(newi)), 36, 64)
	o.tsg = testSeqIDGenerator(x)
	q := o.M.T.NewP()
	q.Name = "2"
	if err := o.M.T.CF().CommitCAS(q); err != nil {
		t.Fatal(err)
	}

	items, err = seqidTestAll(o.M.T, "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 2 {
		t.Fatal(fmt.Sprintf("expected exactly two items, found %d (%+v)", len(items), items))
	}
	if !reflect.DeepEqual(q, items[0]) {
		t.Fatal(fmt.Sprintf("\nexpected: %+v\nreceived: %+v", q, items[0]))
	}
	if !reflect.DeepEqual(p, items[1]) {
		t.Fatal(fmt.Sprintf("\nexpected: %+v\nreceived: %+v", p, items[1]))
	}

	items, err = seqidTestAll(o.M.T, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatal(fmt.Sprintf("expected exactly one item, found %d (%+v)", len(items), items))
	}
	if !reflect.DeepEqual(q, items[0]) {
		t.Fatal(fmt.Sprintf("\nexpected: %+v\nreceived: %+v", q, items[0]))
	}

	x, _ = strconv.ParseUint(string(p.SeqID), 36, 64)
	items, err = seqidTestAll(o.M.T, strconv.FormatUint(x+1, 36), 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 {
		t.Fatal(fmt.Sprintf("expected exactly one item, found %d (%+v)", len(items), items))
	}
	if !reflect.DeepEqual(p, items[0]) {
		t.Fatal(fmt.Sprintf("\nexpected: %+v\nreceived: %+v", p, items[0]))
	}
}
