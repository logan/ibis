package timeline

import "fmt"
import "testing"
import "time"

import "github.com/logan/ibis"

type testModel struct {
	cluster   ibis.Cluster
	lastSeqID *ibis.FakeSeqIDGenerator
	Indexes   *IndexTable
}

func (m *testModel) Close() {
	m.cluster.Close()
}

func newTestModel(t *testing.T) *testModel {
	model := &testModel{
		cluster:   ibis.NewTestConn(t),
		lastSeqID: new(ibis.FakeSeqIDGenerator).Set(36*36*36 - 1),
	}
	schema := ibis.ReflectSchema(model)
	schema.Cluster = model.cluster
	// set up seqid sequence to be 1000, 1001, 1002, ...
	model.Indexes.Provide(ibis.SeqIDGenerator(model.lastSeqID))

	var err error
	if schema.SchemaUpdates, err = ibis.DiffLiveSchema(model.cluster, schema); err != nil {
		t.Fatal(err)
	}
	if err = schema.ApplySchemaUpdates(); err != nil {
		t.Fatal(err)
	}
	return model
}

func scanAllEntries(idx *Index, since ...ibis.TimeUUID) ([]*Entry, error) {
	scanner := idx.Scanner()
	if len(since) > 0 {
		scanner.Since(since[0])
	}
	// read in two at a time
	var entries []*Entry
	page := make([]*Entry, 0, 2)
	scanner.Start()
	for scanner.ScanPage(&page) {
		entries = append(entries, page...)
	}
	return entries, scanner.Error()
}

func checkEntries(entries []*Entry, expected ...ibis.TimeUUID) (string, bool) {
	received := make([]ibis.TimeUUID, len(entries))
	for i, entry := range entries {
		received[i] = entry.ID
	}
	if len(received) != len(expected) {
		return fmt.Sprintf("expected %d entries, received %d: %s", len(expected), len(received),
			received), false
	}
	for i, uuid := range received {
		if uuid != expected[i] {
			return fmt.Sprintf("\nexpected: %+v\nreceived: %+v", expected, received), false
		}
	}
	return "", true
}

func TestIndex(t *testing.T) {
	model := newTestModel(t)
	defer model.Close()

	now := time.Now()
	uuids := []ibis.TimeUUID{
		ibis.UUIDFromTime(now.Add(-3 * time.Minute)),
		ibis.UUIDFromTime(now.Add(-2 * time.Minute)),
		ibis.UUIDFromTime(now.Add(-1 * time.Minute)),
	}

	idx := model.Indexes.Index("Posts", "Published")
	if idx.Name != "Posts:Published" {
		t.Error("expected Posts:Published, got", idx.Name)
	}

	entries, err := scanAllEntries(idx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("how did these get here? %+v", entries)
	}

	for i, uuid := range uuids {
		if err := idx.Add(uuid, i); err != nil {
			t.Error(err)
		}
	}

	entries, err = scanAllEntries(idx)
	if err != nil {
		t.Fatal(err)
	}
	if msg, ok := checkEntries(entries, uuids[2], uuids[1], uuids[0]); !ok {
		t.Error(msg)
	}

	entries, err = scanAllEntries(idx, uuids[2])
	if err != nil {
		t.Fatal(err)
	}
	if msg, ok := checkEntries(entries, uuids[1], uuids[0]); !ok {
		t.Error(msg)
	}
}

func TestPlugin(t *testing.T) {
	type Row struct {
		Name      string        `ibis:"key"`
		Created   ibis.TimeUUID `ibis.timeline:"AllRows, RowsBy(Name)"`
		Published ibis.TimeUUID `ibis.timeline:"PublishedRows, RowsPublishedBy(Name)"`
	}

	type Model struct {
		Indexes *TimelinePlugin
		Rows    *ibis.CF
	}

	model := &Model{Rows: ibis.ReflectCF(Row{})}
	//schema := ibis.ReflectTestSchema(t, model)
	//defer schema.Cluster.Close()
	ibis.ReflectTestSchema(t, model)

	uuid1 := ibis.UUIDFromTime(time.Now())
	row := &Row{Name: "test", Created: uuid1}
	if err := model.Rows.CommitCAS(row); err != nil {
		t.Fatal(err)
	}

	var entry Entry
	if err := model.Indexes.CF.LoadByKey(&entry, "AllRows", uuid1); err != nil {
		t.Fatal(err)
	}
	if uuid1 != entry.ID {
		t.Errorf("expected %s, got %s", uuid1, entry.ID)
	}
	var indexedRow Row
	if err := entry.Decode(&indexedRow); err != nil {
		t.Fatal(err)
	}
	if row.Name != indexedRow.Name {
		t.Errorf("expected %v, got %v", row, indexedRow)
	}

	entry = Entry{}
	if err := model.Indexes.CF.LoadByKey(&entry, "RowsBy:test", uuid1); err != nil {
		t.Fatal(err)
	}
	if uuid1 != entry.ID {
		t.Errorf("expected %s, got %s", uuid1, entry.ID)
	}
}
