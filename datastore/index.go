package datastore

import "errors"
import "fmt"
import "strings"

import "tux21b.org/v1/gocql"

var (
	ErrNoSeqIDListing = errors.New("table has no seqid listing")
)

type seqIDIndexKey int

const SEQID seqIDIndexKey = iota

type SeqIDRow struct {
	ReflectedRow
	SeqID SeqID
}

type CFIndexer interface {
	// IndexName returns a unique name (per indexed column family) for this index.
	IndexName() string

	// IndexCFs returns any auxiliary column families that the indexer intends to use for storage.
	IndexCFs() []*ColumnFamily

	// Index inspects a row's marshalled data returns CQL statements to execute just prior to
	// writing the row itself.
	Index(*ColumnFamily, MarshalledMap) ([]*CQL, error)
}

type SeqIDIndexEntry struct {
	CF             *ColumnFamily
	SeqID          SeqID
	PartitionParts []string
	Key            MarshalledMap
}

func (entry *SeqIDIndexEntry) partition() string {
	if len(entry.PartitionParts) == 0 {
		return interval(entry.SeqID)
	}
	return strings.Join(entry.PartitionParts, "~") + "~" + interval(entry.SeqID)
}

func (entry *SeqIDIndexEntry) GetCF() *ColumnFamily {
	return entry.CF
}

func (entry *SeqIDIndexEntry) Marshal(mmap MarshalledMap) (err error) {
	mmap["Interval"] = &MarshalledValue{TypeInfo: tiVarchar, Dirty: true}
	if mmap["Interval"].Bytes, err = gocql.Marshal(tiVarchar, entry.partition()); err != nil {
		return
	}

	// SeqID must be zero-padded to a length of 13 to collate appropriately.
	mmap["SeqID"] = &MarshalledValue{TypeInfo: tiVarchar, Dirty: true}
	mmap["SeqID"].Bytes, err = gocql.Marshal(tiVarchar, fmt.Sprintf("%013s", entry.SeqID))
	if err != nil {
		return
	}

	// Fill in foreign key.
	for k, v := range entry.Key {
		mmap[k] = v
	}
	return
}

func (entry *SeqIDIndexEntry) Unmarshal(mmap MarshalledMap) error {
	if inter, ok := mmap["Interval"]; ok && inter != nil {
		var s string
		if err := gocql.Unmarshal(tiVarchar, inter.Bytes, &s); err != nil {
			return err
		}
		parts := strings.Split(s, "~")
		if len(parts) > 0 {
			entry.PartitionParts = parts[:len(parts)-1]
		}
	}
	if seqid, ok := mmap["SeqID"]; ok && seqid != nil {
		if err := gocql.Unmarshal(tiVarchar, seqid.Bytes, &entry.SeqID); err != nil {
			return err
		}
	}
	entry.Key = make(MarshalledMap)
	for k, v := range mmap {
		if k != "Interval" && k != "SeqID" {
			entry.Key[k] = v
		}
	}
	return nil
}

type SeqIDIndexer interface {
	SeqIDIndex(MarshalledMap, *SeqIDIndexEntry) error
	IndexName() string
	PartitionKeys() []string
}

var hexDigits = []byte("0123456789abcdef")

func hexenc(bs []byte) string {
	// format each byte as two hexadecimal digits
	enc := make([]byte, len(bs)*2)
	for i, b := range bs {
		enc[2*i] = hexDigits[b/16]
		enc[2*i+1] = hexDigits[b%16]
	}
	return string(enc)
}

type byColsIndexer struct {
	cf      *ColumnFamily
	columns []string
}

func bySeqID(cf *ColumnFamily) SeqIDIndexer {
	return &byColsIndexer{cf: cf}
}

func byCols(cf *ColumnFamily, columns []string) SeqIDIndexer {
	return &byColsIndexer{cf: cf, columns: columns}
}

func (bc *byColsIndexer) IndexName() string {
	if len(bc.columns) == 0 {
		return "BySeqID"
	}
	return "By" + strings.Join(bc.columns, "")
}

func (bc *byColsIndexer) PartitionKeys() []string {
	return bc.columns
}

func (bc *byColsIndexer) SeqIDIndex(mmap MarshalledMap, entry *SeqIDIndexEntry) error {
	var seqid *MarshalledValue
	var ok bool
	if seqid, ok = mmap["SeqID"]; !ok || seqid == nil || len(seqid.Bytes) == 0 {
		s, err := bc.cf.orm.SeqID.New()
		if err != nil {
			return err
		}
		b, err := gocql.Marshal(tiVarchar, string(s))
		if err != nil {
			return err
		}
		seqid = &MarshalledValue{Bytes: b, TypeInfo: tiVarchar, Dirty: true}
		mmap["SeqID"] = seqid
	}
	if err := gocql.Unmarshal(tiVarchar, seqid.Bytes, &entry.SeqID); err != nil {
		return err
	}
	entry.Key = make(MarshalledMap)
	for _, k := range bc.cf.Options.PrimaryKey {
		if k != "SeqID" {
			entry.Key[k] = mmap[k]
		}
	}
	entry.PartitionParts = make([]string, len(bc.columns))
	for i, colname := range bc.columns {
		var s string
		if mv, ok := mmap[colname]; ok && mv != nil {
			s = hexenc(mv.Bytes)
		}
		entry.PartitionParts[i] = s
	}
	return nil
}

type Index struct {
	CF        *ColumnFamily
	IndexedCF *ColumnFamily
	Indexer   SeqIDIndexer
}

func (idx *Index) IndexName() string {
	return idx.Indexer.IndexName()
}

func (idx *Index) IndexCFs() []*ColumnFamily {
	if idx.CF == nil {
		idx.CF = &ColumnFamily{
			Name: idx.IndexedCF.Name + idx.IndexName(),
			Columns: []Column{
				Column{
					Name: "Interval", Type: "varchar",
					typeInfo: tiVarchar,
				},
				Column{
					Name: "SeqID", Type: "varchar",
					typeInfo: tiVarchar,
				},
			},
		}
		for i, k := range idx.IndexedCF.Options.PrimaryKey {
			if k != "SeqID" {
				idx.CF.Columns = append(idx.CF.Columns, idx.IndexedCF.Columns[i])
			}
		}
		idx.CF.Options = NewCFOptions(idx.CF).Key("Interval", "SeqID")
		idx.CF.Options.OnCreate(insertSeqIDListingSentinel)
	}
	return []*ColumnFamily{idx.CF}
}

func (idx *Index) Index(cf *ColumnFamily, mmap MarshalledMap) ([]*CQL, error) {
	// TODO: handle index deletions when mutable columns are indexed
	entry := &SeqIDIndexEntry{CF: idx.CF}
	if err := idx.Indexer.SeqIDIndex(mmap, entry); err != nil {
		return nil, err
	}
	return idx.CF.PrepareCommit(entry)
}

func (idx *Index) Iter() *IndexIter {
	return &IndexIter{
		rowcf:         idx.IndexedCF,
		idxcf:         idx.CF,
		partitionKeys: idx.Indexer.PartitionKeys(),
	}
}

func insertSeqIDListingSentinel(orm *Orm, table *ColumnFamily) error {
	interval := orm.SeqID.CurrentInterval()
	q := orm.Query(fmt.Sprintf("INSERT INTO %s (Interval, SeqID) VALUES (?, ?)", table.Name),
		interval, "")
	return q.Exec()
}

type IndexIter struct {
	// need something in here to represent which partitions of a compound index to scan
	// probably just prefix string, but need an API to give it
	// probably in Iter(), but it probably ought to type check
	After         SeqID
	LowerBound    SeqID
	Limit         int
	ChunkSize     int
	Err           error
	rowcf         *ColumnFamily
	idxcf         *ColumnFamily
	keysRetrieved int
	partitionKeys []string
	prefix        string
	interval      string
	rowchan       chan MarshalledMap
	keychan       chan SeqIDIndexEntry
	exhausted     bool
}

func (iter *IndexIter) marshalPartitionPart(col *Column, part interface{}) string {
	b, err := gocql.Marshal(col.typeInfo, part)
	if err != nil {
		iter.Err = err
		return ""
	}
	return hexenc(b)
}

func (iter *IndexIter) By(where ...interface{}) *IndexIter {
	if len(where) > 0 {
		parts := make([]string, len(where))
		for i, x := range where {
			if iter.Err == nil && i < len(iter.partitionKeys) {
				for _, col := range iter.rowcf.Columns {
					if col.Name == iter.partitionKeys[i] {
						parts[i] = iter.marshalPartitionPart(&col, x)
					}
				}
			}
		}
		iter.prefix = strings.Join(parts, "~") + "~"
	}
	return iter
}

// Next reads the next item from an iteration over a SeqIDListing. The item is read into the given
// row object. If this function returns false, compare iter.Err to nil to determine whether an error
// occurred or the iteration over the listing was merely exhausted.
func (iter *IndexIter) Next(row Row) bool {
	if iter.Err != nil {
		return false
	}
	if !iter.rowcf.IsValidRowType(row) {
		iter.Err = ErrInvalidType
		return false
	}
	if iter.rowchan == nil {
		iter.scanChunk()
	}
	if mmap, ok := <-iter.rowchan; ok {
		iter.Err = row.Unmarshal(mmap)
		return iter.Err == nil
	}
	return false
}

func (iter *IndexIter) scanChunk() {
	if iter.ChunkSize == 0 {
		iter.ChunkSize = 10000
	}

	iter.rowchan = make(chan MarshalledMap, iter.ChunkSize)

	go func() {
		// TODO: do batch lookups
		defer close(iter.rowchan)

		for !iter.exhausted && iter.Err == nil {
			if iter.keychan == nil {
				iter.scanInterval()
				if iter.Err != nil {
					return
				}
			}
			for entry := range iter.keychan {
				cql := NewSelect(iter.rowcf)
				for k, v := range entry.Key {
					cql.Where(k+" = ?", v)
				}
				mmap := make(MarshalledMap)
				if iter.Err = cql.Query().Scan(mmap); iter.Err != nil {
					return
				}
				iter.rowchan <- mmap
			}
		}
	}()
}

func (iter *IndexIter) scanInterval() {
	iter.keychan = make(chan SeqIDIndexEntry, iter.ChunkSize)

	go func() {
		defer close(iter.keychan)

		for !iter.exhausted {
			var limit int
			if iter.Limit != 0 {
				limit = iter.Limit - iter.keysRetrieved
				if limit <= 0 {
					return
				}
				if limit > 10000 {
					limit = 10000
				}
			} else {
				limit = 10000
			}

			ci := iter.queryCurrentInterval(limit)
			for {
				entry := SeqIDIndexEntry{CF: iter.rowcf}
				mmap := make(MarshalledMap)
				entry.Marshal(mmap)
				if !ci.Next(mmap) {
					break
				}
				if iter.Err = entry.Unmarshal(mmap); iter.Err != nil {
					return
				}
				if entry.SeqID == "" {
					// encountered sentinel, scan is complete
					iter.exhausted = true
					return
				}
				iter.keysRetrieved++
				iter.keychan <- entry
			}
			if iter.Err = ci.Close(); iter.Err != nil {
				// abort with error
				return
			}
			if iter.interval == "0" {
				iter.exhausted = true
			}
			iter.interval = decrInterval(iter.interval)
			if iter.Limit > 0 && iter.keysRetrieved >= iter.Limit {
				iter.exhausted = true
			}
		}
	}()
	return
}

func (iter *IndexIter) queryCurrentInterval(limit int) *CQLIter {
	if iter.interval == "" {
		if iter.After == "" {
			iter.interval = iter.rowcf.orm.SeqID.CurrentInterval()
			iter.After = intervalToSeqID(incrInterval(iter.interval))
		} else {
			iter.interval = interval(iter.After)
		}
	}

	// TODO: support compound partitions
	cql := NewSelect(iter.idxcf)
	cql.Where("Interval = ?", iter.prefix+iter.interval)
	cql.Where("SeqID < ?", fmt.Sprintf("%013s", iter.After))
	cql.OrderBy("SeqID DESC").Limit(limit)
	return cql.Query().Iter()
}

func IndexBy(t *ColumnFamily, cols ...string) *Index {
	idx, _ := t.Options.indexMap["By"+strings.Join(cols, "")]
	return idx
}

func IndexBySeqID(t *ColumnFamily) *Index {
	return IndexBy(t, "SeqID")
}
