package ibis

import "errors"
import "reflect"

type ColumnTagApplier interface {
	ApplyTag(tagValue string, cf *CF, col Column) error
}

type ColumnTags struct {
	tags map[string]ColumnTagApplier
}

func (t *ColumnTags) Register(name string, applier ColumnTagApplier) {
	if t.tags == nil {
		t.tags = make(map[string]ColumnTagApplier)
	}
	t.tags[name] = applier
}

func (t *ColumnTags) applyAll(tag reflect.StructTag, cf *CF, col Column) []error {
	errors := make([]error, 0)
	for name, applier := range t.tags {
		if name == "" {
			name = "ibis"
		} else {
			name = "ibis." + name
		}
		val := tag.Get(name)
		if val != "" {
			if err := applier.ApplyTag(val, cf, col); err != nil {
				errors = append(errors, err)
			}
		}
	}
	return errors
}

type SchemaPlugin interface {
	RegisterColumnTags(*ColumnTags)
}

type defaultPlugin int

func (plugin defaultPlugin) RegisterColumnTags(tags *ColumnTags) {
	tags.Register("", plugin)
}

func (plugin defaultPlugin) ApplyTag(value string, cf *CF, col Column) error {
	switch {
	case value == "key":
		if cf.primaryKey == nil {
			cf.primaryKey = []string{col.Name}
		} else {
			cf.primaryKey = append(cf.primaryKey, col.Name)
		}
	default:
		return errors.New("invalid tag: " + value)
	}
	return nil
}

type MarshalPlugin interface {
	OnMarshal(MarshaledMap) error
	OnUnmarshal(MarshaledMap) error
}

// AutoPatcher provides a plugin for "patched" commits of rows. When you load a row and modify a
// single field, only that modified field will be committed.
//
// Simply include field of type *AutoPatcher in your reflected row struct to activate this behavior.
// To force a full commit, call the InvalidateAll() method.
type AutoPatcher MarshaledMap

func (p *AutoPatcher) OnMarshal(mmap MarshaledMap) error {
	if *p != nil {
		// Original values are known in p. Fill in OriginalBytes throughout mmap.
		for k, v := range *p {
			mv := mmap[k]
			if mv == nil {
				if v != nil {
					mv = &MarshaledValue{TypeInfo: v.TypeInfo}
				}
			}
			if v != nil {
				mv.OriginalBytes = v.Bytes
			}
		}
	}
	return nil
}

func (p *AutoPatcher) OnUnmarshal(mmap MarshaledMap) error {
	*p = AutoPatcher{}
	for k, v := range mmap {
		if v != nil {
			(*p)[k] = &MarshaledValue{Bytes: v.Bytes, OriginalBytes: v.Bytes, TypeInfo: v.TypeInfo}
		}
	}
	return nil
}
