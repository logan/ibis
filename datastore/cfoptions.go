package datastore

type OnCreateHook func(*Orm, *ColumnFamily) error

// CFOptions is used to provide additional properties for a column family definition.
type CFOptions struct {
	CF            *ColumnFamily
	PrimaryKey    []string
	onCreateHooks []OnCreateHook // If given, will be called immediately after table creation.
	typeID        int
}

func NewCFOptions(cf *ColumnFamily) *CFOptions {
	o := &CFOptions{CF: cf}
	o.onCreateHooks = make([]OnCreateHook, 0)
	return o
}

func (o *CFOptions) Key(keys ...string) *CFOptions {
	o.PrimaryKey = keys

	// primary key columns must come first and in order
	rearranged := make([]Column, len(o.CF.Columns))
	keymap := make(map[string]bool)
	for i, k := range keys {
		for _, col := range o.CF.Columns {
			if k == col.Name {
				keymap[k] = true
				rearranged[i] = col
				break
			}
		}
	}
	i := len(keys)
	for _, col := range o.CF.Columns {
		if _, ok := keymap[col.Name]; !ok {
			rearranged[i] = col
			i++
		}
	}
	copy(o.CF.Columns, rearranged)

	return o
}

func (o *CFOptions) OnCreate(hook OnCreateHook) *CFOptions {
	o.onCreateHooks = append(o.onCreateHooks, hook)
	return o
}
