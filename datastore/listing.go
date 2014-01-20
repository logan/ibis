package datastore

import "errors"
import "fmt"
import "reflect"
import "strings"

import "tux21b.org/v1/gocql"

var (
	ErrNoSeqIDListing = errors.New("table has no seqid listing")
)

type SeqIDPersistent struct {
	Persistent
	SeqID SeqID
}

func SeqIDListingColumnFamily(seqid_table *ColumnFamily) *ColumnFamily {
	table := &ColumnFamily{
		Name: fmt.Sprintf("%sBySeqID", seqid_table.Name),
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
		Options: CFOptions{
			PrimaryKey: []string{"Interval", "SeqID"},
			OnCreate:   insertSeqIDListingSentinel,
		},
	}
	table.Columns = append(table.Columns,
		seqid_table.Columns[:len(seqid_table.Options.PrimaryKey)]...)
	return table
}

func insertSeqIDListingSentinel(orm *Orm, table *ColumnFamily) error {
	interval := orm.SeqID.CurrentInterval()
	q := orm.Query(fmt.Sprintf("INSERT INTO %s (Interval, SeqID) VALUES (?, ?)", table.Name),
		interval, "")
	return q.Exec()
}

func addToSeqIDListing(orm *Orm, table *ColumnFamily, seqid SeqID, rowValues RowValues) error {
	stmt := fmt.Sprintf("INSERT INTO %s (Interval, SeqID, %s) VALUES (%s)",
		table.seqIDTable.Name, strings.Join(table.Options.PrimaryKey, ", "),
		placeholderList(len(table.seqIDTable.Columns)))
	values := make([]*RowValue, len(table.Options.PrimaryKey))
	for i, pk_name := range table.Options.PrimaryKey {
		values[i] = rowValues[pk_name]
	}
	valueints := make([]interface{}, len(values)+2)
	valueints[0] = interval(seqid)
	valueints[1] = seqid
	for i, v := range values {
		valueints[i+2] = v
	}
	q := orm.Query(stmt, valueints...)
	return q.Exec()
}

type SeqIDListingIter struct {
	After         SeqID
	Limit         int
	ChunkSize     int
	Err           error
	rcf           ReflectableColumnFamily
	cf            *ColumnFamily
	keysRetrieved int
	interval      string
	rowchan       chan Persistable
	keychan       chan []interface{}
	exhausted     bool
}

// IterSeqIDListing creates a listing iterator over a bound table with a seqid listing.
func IterSeqIDListing(table ReflectableColumnFamily) *SeqIDListingIter {
	cf := table.NewRow().GetCF()
	var err error
	if !cf.IsBound() {
		err = ErrTableNotBound
	} else if cf.seqIDTable == nil {
		err = ErrNoSeqIDListing
	}
	return &SeqIDListingIter{rcf: table, cf: cf, Err: err}
}

// Next reads the next item from an iteration over a SeqIDListing. The item is read into the given
// row object. If this function returns false, compare iter.Err to nil to determine whether an error
// occurred or the iteration over the listing was merely exhausted.
func (iter *SeqIDListingIter) Next(row Persistable) (ok bool) {
	if iter.Err != nil {
		return false
	}
	if !iter.cf.IsValidRowType(row) {
		iter.Err = ErrInvalidType
		return false
	}
	if iter.rowchan == nil {
		iter.scanChunk(row)
	}
	r, ok := <-iter.rowchan
	if ok {
		reflect.ValueOf(row).Elem().Set(reflect.ValueOf(r).Elem())
	}
	return
}

func (iter *SeqIDListingIter) scanChunk(row Persistable) {
	if iter.ChunkSize == 0 {
		iter.ChunkSize = 10000
	}

	iter.rowchan = make(chan Persistable, iter.ChunkSize)

	go func() {
		// TODO: do batch lookups
		defer close(iter.rowchan)

		row_type := reflect.TypeOf(row)
		buf := reflect.MakeSlice(reflect.SliceOf(row_type), iter.ChunkSize, iter.ChunkSize)
		for i := 0; i < iter.ChunkSize; i++ {
			buf.Index(i).Set(reflect.New(row_type.Elem()))
		}
		buf_i := 0

		for !iter.exhausted {
			if iter.keychan == nil {
				iter.scanInterval(row)
				if iter.Err != nil {
					return
				}
			}
			for pki := range iter.keychan {
				row := iter.rcf.NewRow()
				if iter.Err = iter.cf.LoadByKey(row, pki...); iter.Err != nil {
					return
				}
				buf.Index(buf_i).Elem().Set(reflect.ValueOf(row).Convert(row_type).Elem())
				iter.rowchan <- row
				buf_i = (buf_i + 1) % buf.Len()
			}
		}
	}()
}

func (iter *SeqIDListingIter) scanInterval(row Persistable) {
	iter.keychan = make(chan []interface{}, iter.ChunkSize)

	go func() {
		var interval string
		var seqid SeqID

		defer close(iter.keychan)

		buf := make([][]interface{}, iter.ChunkSize)
		rvs := make([][]RowValue, iter.ChunkSize)
		buf_i := 0

		for {
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
				buf[buf_i] = make([]interface{}, len(iter.cf.Options.PrimaryKey)+2)
				buf[buf_i][0] = &interval
				buf[buf_i][1] = &seqid
				rvs[buf_i] = make([]RowValue, len(iter.cf.Options.PrimaryKey))
				for i, _ := range iter.cf.Options.PrimaryKey {
					buf[buf_i][i+2] = &rvs[buf_i][i]
				}
				if !ci.Scan(buf[buf_i]...) {
					break
				}
				if seqid == "" {
					// encountered sentinel, scan is complete
					iter.exhausted = true
					return
				}
				iter.After = seqid
				iter.keysRetrieved++
				iter.keychan <- buf[buf_i][2:]
				buf_i = (buf_i + 1) % len(buf)
			}
			if iter.Err = ci.Close(); iter.Err != nil {
				// abort with error
				return
			}
			iter.interval = decrInterval(iter.interval)
			if iter.Limit > 0 && iter.keysRetrieved >= iter.Limit {
				iter.exhausted = true
			}
		}
	}()
	return
}

func (iter *SeqIDListingIter) queryCurrentInterval(limit int) *gocql.Iter {
	if iter.interval == "" {
		if iter.After == "" {
			iter.interval = iter.cf.orm.SeqID.CurrentInterval()
			iter.After = intervalToSeqID(incrInterval(iter.interval))
		} else {
			iter.interval = interval(iter.After)
		}
	}
	stmt := fmt.Sprintf("SELECT * FROM %s WHERE Interval = ? AND SeqID < ?"+
		" ORDER BY SeqID DESC LIMIT %d", iter.cf.seqIDTable.Name, limit)
	q := iter.cf.orm.Query(stmt, iter.interval, iter.After)
	i := q.Iter()
	return i
}
