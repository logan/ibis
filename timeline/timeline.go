package timeline

import "errors"
import "encoding/json"
import "fmt"
import "reflect"
import "strings"
import "time"

import "tux21b.org/v1/gocql"

import "github.com/logan/ibis"

type Entry struct {
	Partition  string `ibis:"key"`
	ibis.SeqID `ibis:"key"`
	Bytes      []byte
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

type IndexTable struct {
	*ibis.CF
}

func (t *IndexTable) NewCF() *ibis.CF {
	t.CF = ibis.ReflectCF(Entry{})
	t.Provide(IndexProvider(t))
	return t.CF
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

type EntryChannel chan *Entry

type IndexScanner struct {
	EntryChannel
	since     ibis.SeqID
	fetched   int
	limit     int
	index     *Index
	query     ibis.CFQuery
	exhausted bool
	err       error
}

func NewIndexScanner(index *Index) *IndexScanner {
	scanner := &IndexScanner{index: index}
	return scanner
}

func (scanner *IndexScanner) Since(seqid ibis.SeqID) *IndexScanner {
	if seqid != "" {
		seqid = seqid.Pad()
	}
	scanner.since = seqid
	return scanner
}

func (scanner *IndexScanner) Limit(limit int) *IndexScanner {
	scanner.limit = limit
	return scanner
}

func (scanner *IndexScanner) Start() EntryChannel {
	if scanner.EntryChannel != nil {
		close(scanner.EntryChannel)
	}
	scanner.EntryChannel = make(EntryChannel)
	scanner.fetched = 0
	go scanner.scan()
	return scanner.EntryChannel
}

func (scanner *IndexScanner) Error() error {
	return scanner.err
}

func (scanner *IndexScanner) start() ibis.CFQuery {
	if scanner.since == "" {
		// If no since is given, generate the next SeqID to start a scan from right now.
		var gen ibis.SeqIDGenerator
		if scanner.index.Table.GetProvider(&gen) {
			next, err := gen.NewSeqID()
			if err == nil {
				scanner.since = next.Pad()
			} else {
				scanner.err = err
			}
		}
	}
	cql := ibis.Select().From(scanner.index.Table.CF).
		Where("Partition = ?", scanner.index.Name).
		Where("SeqID < ?", scanner.since).
		OrderBy("SeqID DESC")
	if scanner.limit != 0 {
		cql.Limit(scanner.limit)
	}
	q := cql.Query()
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

func (scanner *IndexScanner) ScanPage(x interface{}) bool {
	if scanner.exhausted || scanner.err != nil {
		return false
	}
	ptrType := reflect.TypeOf(x)
	if ptrType.Kind() != reflect.Ptr || ptrType.Elem().Kind() != reflect.Slice {
		scanner.err = errors.New(fmt.Sprintf("ScanPage needs pointer to slice, not %T", ptrType))
		return false
	}
	sliceType := ptrType.Elem()
	ptrValue := reflect.ValueOf(x)
	sliceValue := ptrValue.Elem()
	var sliceSize int
	if sliceValue.IsNil() {
		sliceSize = 1000
		sliceValue.Set(reflect.MakeSlice(sliceType, 0, sliceSize))
	} else {
		sliceSize = sliceValue.Cap()
		sliceValue.SetLen(0)
	}
	for i := 0; i < sliceSize && scanner.err == nil; i++ {
		row, ok := <-scanner.EntryChannel
		if !ok {
			scanner.exhausted = true
			return i > 0
		}
		sliceValue.SetLen(i + 1)
		sliceValue.Index(i).Set(reflect.ValueOf(row))
	}
	return scanner.err == nil
}

type TimelinePlugin struct {
	*IndexTable

	// TODO: Fix this type it's ridiculous
	timelineDefs map[string]map[string][]timelineDef
}

func (plugin *TimelinePlugin) NewCF() *ibis.CF {
	plugin.IndexTable = new(IndexTable)
	cf := plugin.IndexTable.NewCF()
	cf.Provide(ibis.Plugin(plugin))
	return cf
}

func (plugin *TimelinePlugin) RegisterColumnTags(tags *ibis.ColumnTags) {
	tags.Register("timeline", plugin)
}

func (plugin *TimelinePlugin) ApplyTag(value string, cf *ibis.CF, col *ibis.Column) error {
	defs, err := parseTimelineDefs(value)
	if defs != nil {
		if plugin.timelineDefs == nil {
			plugin.timelineDefs = make(map[string]map[string][]timelineDef)
		}
		_, ok := plugin.timelineDefs[cf.Name()]
		if !ok {
			plugin.timelineDefs[cf.Name()] = make(map[string][]timelineDef)
			cf.Precommit(plugin.precommit)
		}
		plugin.timelineDefs[cf.Name()][col.Name] = defs
	}
	fmt.Printf("timelines: %+v\n", plugin.timelineDefs)
	return err
}

func (plugin *TimelinePlugin) precommit(mmap ibis.MarshalledMap) ([]ibis.CQL, error) {
	cqls := make([]ibis.CQL, 0)
	for colName, _ := range plugin.timelineDefs {
		mv := mmap[colName]
		if mv.Dirty() && mv.TypeInfo == ibis.TITimestamp {
			var newT, oldT time.Time
			if mv.Bytes != nil {
				if err := gocql.Unmarshal(ibis.TITimestamp, mv.Bytes, &newT); err != nil {
					return nil, err
				}
			}
			if mv.OriginalBytes != nil {
				if err := gocql.Unmarshal(ibis.TITimestamp, mv.OriginalBytes, &oldT); err != nil {
					return nil, err
				}
			}
			// get the seqid
			cqls = append(cqls, plugin.onTimestampChange("", colName, oldT, newT)...)
		}
	}
	return cqls, nil
}

func (plugin *TimelinePlugin) onTimestampChange(seqid ibis.SeqID, colName string,
	oldT, newT time.Time) []ibis.CQL {
	/*

	   cqls := make([]ibis.CQL, 0)
	   defs := plugin.timelineDefs[colName]
	*/
	// TODO: if !oldT.IsZero() { /* remove */ }
	/*
	   if !newT.IsZero() {
	       for _, def := range defs {
	           idx := plugin.IndexTable.Index(def.keys()...)
	           cqls = append(cqls, idx.MakeAdd(seqid,
	       }
	   }
	*/
	return []ibis.CQL{}
}
