package timeline

import "github.com/logan/ibis"

import "fmt"
import "testing"

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
	schema.SetCluster(model.cluster)
	// set up seqid sequence to be 1000, 1001, 1002, ...
	model.Indexes.SeqIDGenerator = model.lastSeqID

	var err error
	if schema.SchemaUpdates, err = ibis.DiffLiveSchema(model.cluster, schema); err != nil {
		t.Fatal(err)
	}
	if err = schema.ApplySchemaUpdates(); err != nil {
		t.Fatal(err)
	}
	return model
}

func scanAllEntries(idx *Index, since ...string) ([]*Entry, error) {
	scanner := idx.Scanner()
	if len(since) > 0 {
		scanner.Since(ibis.SeqID(since[0]))
	}
	// read in two at a time
	//entries := make([]*Entry, 0)
	var entries []*Entry
	page := make([]*Entry, 0, 2)
	scanner.Start()
	for scanner.ScanPage(&page) {
		entries = append(entries, page...)
	}
	return entries, scanner.Error()
}

func checkEntries(entries []*Entry, seqids ...string) (string, bool) {
	received := make([]ibis.SeqID, len(entries))
	for i, entry := range entries {
		received[i] = entry.SeqID
	}
	if len(received) != len(seqids) {
		return fmt.Sprintf("expected %d entries, received %d: %s", len(seqids), len(received),
			received), false
	}
	for i, seqid := range received {
		if seqid != ibis.SeqID(seqids[i]).Pad() {
			return fmt.Sprintf("entries have different SeqIDs than expected: %s", received), false
		}
	}
	return "", true
}

func TestIndex(t *testing.T) {
	model := newTestModel(t)
	defer model.Close()

	idx := model.Indexes.Index("Posts", "Published")
	if idx.Name != "PostsPublished" {
		t.Error("expected PostsPublished, got", idx.Name)
	}

	entries, err := scanAllEntries(idx)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 0 {
		t.Errorf("how did these get here? %+v", entries)
	}

	newSeqID := func() ibis.SeqID {
		seqid, _ := model.Indexes.SeqIDGenerator.NewSeqID()
		return seqid
	}
	if err := idx.Add(newSeqID(), []byte{0}); err != nil {
		t.Error(err)
	}
	if err := idx.Add(newSeqID(), []byte{1}); err != nil {
		t.Error(err)
	}
	if err := idx.Add(newSeqID(), []byte{2}); err != nil {
		t.Error(err)
	}

	entries, err = scanAllEntries(idx)
	if err != nil {
		t.Fatal(err)
	}
	if msg, ok := checkEntries(entries, "1002", "1001", "1000"); !ok {
		t.Error(msg)
	}

	entries, err = scanAllEntries(idx, "1002")
	if err != nil {
		t.Fatal(err)
	}
	if msg, ok := checkEntries(entries, "1001", "1000"); !ok {
		t.Error(msg)
	}
}
