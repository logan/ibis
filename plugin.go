package ibis

import "errors"
import "reflect"

type ColumnTagApplier interface {
	ApplyTag(tagValue string, cf *CF, col *Column) error
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

func (t *ColumnTags) applyAll(tag reflect.StructTag, cf *CF, col *Column) []error {
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

type Plugin interface {
	RegisterColumnTags(*ColumnTags)
}

type defaultPlugin int

func (plugin defaultPlugin) RegisterColumnTags(tags *ColumnTags) {
	tags.Register("", plugin)
}

func (plugin defaultPlugin) ApplyTag(value string, cf *CF, col *Column) error {
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
