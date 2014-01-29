package timeline

import "encoding/json"
import "strings"

import "github.com/logan/ibis"

type IndexTable struct {
	*ibis.ColumnFamily
}

func (t *IndexTable) CF() *ibis.ColumnFamily {
	t.ColumnFamily = ibis.ReflectColumnFamily(Entry{})
	t.Provide(IndexProvider(t))
	return t.Key("Partition", "SeqID")
}

func (t *IndexTable) Index(keys ...string) *Index {
	return &Index{Table: t, Name: strings.Join(keys, "")}
}

type IndexProvider interface {
	Index(keys ...string) *Index
}

type Index struct {
	Table *IndexTable
	Name  string
}

func (idx *Index) Add(seqid ibis.SeqID, v interface{}) error {
	cql, err := idx.MakeAdd(seqid, v)
	if err != nil {
		return err
	}
	return cql.Query().Exec()
}

func (idx *Index) MakeAdd(seqid ibis.SeqID, v interface{}) (ibis.CQL, error) {
	enc, err := json.Marshal(v)
	if err != nil {
		return ibis.CQL{}, err
	}
	entry := &Entry{SeqID: seqid.Pad(), Bytes: enc}
	entry.encodePartition(idx.Name)
	// TODO: write with timestamp
	return idx.Table.MakeCommit(entry)
}

// TODO: add prefetch options
func (idx *Index) Scanner() *IndexScanner {
	return NewIndexScanner(idx)
}

type Entry struct {
	ibis.SeqID
	Partition string
	Bytes     []byte
}

func (e *Entry) encodePartition(name string) {
	// TODO: add bucket and shard as args
	e.Partition = name
}

func (e *Entry) decodePartition() string {
	// TODO: add bucket and shard to compound return value
	return e.Partition
}

func (e *Entry) Decode(v interface{}) error {
	return json.Unmarshal(e.Bytes, v)
}

type EntryChannel chan *Entry

type IndexScanner struct {
	EntryChannel
	since ibis.SeqID
	index *Index
	query ibis.CFQuery
	err   error
}

func NewIndexScanner(index *Index) *IndexScanner {
	scanner := &IndexScanner{index: index}
	return scanner
}

func (scanner *IndexScanner) Since(seqid ibis.SeqID) {
	if seqid != "" {
		seqid = seqid.Pad()
	}
	scanner.since = seqid
}

func (scanner *IndexScanner) Start() EntryChannel {
	if scanner.EntryChannel != nil {
		close(scanner.EntryChannel)
	}
	scanner.EntryChannel = make(EntryChannel)
	go scanner.scan()
	return scanner.EntryChannel
}

func (scanner *IndexScanner) Error() error {
	return scanner.err
}

func (scanner *IndexScanner) start() ibis.CFQuery {
	if scanner.since == "" {
		// If no since is given, generate the next SeqID to start a scan from right now.
		next, err := scanner.index.Table.NewSeqID()
		if err == nil {
			scanner.since = next.Pad()
		} else {
			scanner.err = err
		}
	}
	q := ibis.Select().From(scanner.index.Table.ColumnFamily).
		Where("Partition = ?", scanner.index.Name).
		Where("SeqID < ?", scanner.since).
		OrderBy("SeqID DESC").
		Query()
	return scanner.index.Table.Scanner(q)
}

func (scanner *IndexScanner) scan() {
	defer close(scanner.EntryChannel)
	scanner.query = scanner.start()
	for scanner.err == nil {
		entry := new(Entry)
		if !scanner.query.ScanRow(entry) {
			scanner.err = scanner.query.Close()
			return
		}
		scanner.since = entry.SeqID
		scanner.EntryChannel <- entry
	}
}
