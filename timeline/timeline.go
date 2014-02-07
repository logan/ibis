package timeline

import "errors"
import "encoding/json"
import "fmt"
import "reflect"
import "strings"
import "time"

import "github.com/gocql/gocql"
import "github.com/logan/ibis"

var Fuzz = 5 * time.Minute

type Entry struct {
	Partition string        `ibis:"key"`
	ID        ibis.TimeUUID `ibis:"key"`
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

type IndexTable struct {
	*ibis.CF
}

func (t *IndexTable) NewCF() (*ibis.CF, error) {
	var err error
	t.CF, err = ibis.ReflectCF(Entry{})
	t.Provide(IndexProvider(t))
	return t.CF, err
}

func (t *IndexTable) Index(keys ...string) *Index {
	var name string
	if len(keys) > 0 {
		if len(keys) > 1 {
			name = keys[0] + ":" + strings.Join(keys[1:], "")
		} else {
			name = keys[0]
		}
	}
	return &Index{Table: t, Name: name}
}

type IndexProvider interface {
	Index(keys ...string) *Index
}

type Index struct {
	Table *IndexTable
	Name  string
}

func (idx *Index) Add(uuid ibis.TimeUUID, v interface{}) error {
	cql, err := idx.MakeAdd(uuid, v)
	if err != nil {
		return err
	}
	return cql.Query().Exec()
}

func (idx *Index) MakeAdd(uuid ibis.TimeUUID, v interface{}) (ibis.CQL, error) {
	enc, err := json.Marshal(v)
	if err != nil {
		return ibis.CQL{}, err
	}
	entry := &Entry{ID: uuid, Bytes: enc}
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
	since     ibis.TimeUUID
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

func (scanner *IndexScanner) Since(uuid ibis.TimeUUID) *IndexScanner {
	scanner.since = uuid
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
	if !scanner.since.IsSet() {
		scanner.since = ibis.UUIDFromTime(time.Now().Add(Fuzz))
	}
	cql := ibis.Select().From(scanner.index.Table.CF).
		Where("Partition = ?", scanner.index.Name).
		Where("ID < ?", scanner.since).
		OrderBy("ID DESC")
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
		scanner.since = entry.ID
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

	precommitters map[string]*timelinePrecommitter
}

func (plugin *TimelinePlugin) NewCF() (*ibis.CF, error) {
	var err error
	plugin.IndexTable = new(IndexTable)
	cf, err := plugin.IndexTable.NewCF()
	cf.Provide(ibis.SchemaPlugin(plugin))
	cf.Provide(IndexProvider(plugin))
	return cf, err
}

func (plugin *TimelinePlugin) RegisterColumnTags(tags *ibis.ColumnTags) {
	tags.Register("timeline", plugin)
}

func (plugin *TimelinePlugin) ApplyTag(value string, cf *ibis.CF, col ibis.Column) error {
	defs, err := parseTimelineDefs(value)
	if defs != nil {
		if plugin.precommitters == nil {
			plugin.precommitters = make(map[string]*timelinePrecommitter)
		}
		hook, ok := plugin.precommitters[cf.Name()]
		if !ok {
			hook = &timelinePrecommitter{cf, plugin.IndexTable, make(map[string][]timelineDef)}
			cf.Precommit(hook.precommit)
			plugin.precommitters[cf.Name()] = hook
		}
		hook.timelineDefs[col.Name] = defs
	}
	return err
}

type timelinePrecommitter struct {
	*ibis.CF
	*IndexTable
	timelineDefs map[string][]timelineDef
}

func (hook *timelinePrecommitter) precommit(row interface{}, mmap ibis.MarshaledMap) (
	[]ibis.CQL, error) {
	cqls := make([]ibis.CQL, 0)
	for colName, defs := range hook.timelineDefs {
		mv := mmap[colName]
		if mv.Dirty() && mv.TypeInfo == ibis.TIUUID {
			var newU, oldU gocql.UUID
			if mv.Bytes != nil {
				if err := gocql.Unmarshal(ibis.TIUUID, mv.Bytes, &newU); err != nil {
					return nil, err
				}
			}
			if mv.OriginalBytes != nil {
				if err := gocql.Unmarshal(ibis.TIUUID, mv.OriginalBytes, &oldU); err != nil {
					return nil, err
				}
			}
			cs, err := hook.onUUIDChange(row, mmap, ibis.TimeUUID(oldU), ibis.TimeUUID(newU), defs)
			if err != nil {
				return nil, err
			}
			cqls = append(cqls, cs...)
		}
	}
	return cqls, nil
}

func (hook *timelinePrecommitter) onUUIDChange(row interface{}, mmap ibis.MarshaledMap,
	oldU, newU ibis.TimeUUID, defs []timelineDef) ([]ibis.CQL, error) {
	cqls := make([]ibis.CQL, 0)
	if newU.IsSet() {
		for _, def := range defs {
			keys := []string{def.name}
			if def.by != nil {
				for _, k := range def.by {
					var key string
					mv := mmap[k]
					if mv != nil && mv.TypeInfo == ibis.TIVarchar {
						if err := gocql.Unmarshal(ibis.TIVarchar, mv.Bytes, &key); err != nil {
							return nil, err
						}
					}
					keys = append(keys, key)
				}
			}
			idx := hook.IndexTable.Index(keys...)
			cql, err := idx.MakeAdd(newU, row)
			if err != nil {
				return nil, err
			}
			cqls = append(cqls, cql)
		}
	}
	return cqls, nil
}
