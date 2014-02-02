package timeline

import "errors"
import "fmt"
import "unicode"

type timelineDef struct {
	name string
	by   []string
}

func (def timelineDef) keys() []string {
	if def.by == nil || len(def.by) == 0 {
		return []string{def.name}
	} else {
		ks := make([]string, 1, 1+len(def.by))
		ks[0] = def.name
		return append(ks, def.by...)
	}
}

func parseTimelineDefs(text string) ([]timelineDef, error) {
	t := list(pTag, pComma, token{runes: []rune(text)})
	if t.err != nil {
		return nil, t.err
	}
	ctxs := t.ctx.([]interface{})
	defs := make([]timelineDef, len(ctxs))
	for i, ctx := range ctxs {
		defs[i] = ctx.(timelineDef)
	}
	if t.eof {
		return defs, nil
	} else {
		return defs, errors.New("ignoring trailing garbage")
	}
}

type parser func(token) token

type token struct {
	runes []rune
	pos   int
	eof   bool
	err   error
	ctx   interface{}
}

func (t token) fail(values ...interface{}) token {
	t.err = errors.New(fmt.Sprint(values...))
	return t
}

func (t token) failf(format string, values ...interface{}) token {
	t.err = errors.New(fmt.Sprintf(format, values...))
	return t
}

func (t token) advance(n int) token {
	u := t
    u.runes = u.runes[n:]
	u.pos += n
	return u
}

func (t token) with(v interface{}) token {
	t.ctx = v
	return t
}

func pTag(t token) token {
	if t = pIdent(t); t.err != nil {
		return t
	}
	id := t.ctx.(string)
	u := pRune(t, '(')
	if u.err != nil {
		return t.with(timelineDef{name: id})
	}

	if u = list(pIdent, pComma, u); u.err != nil {
		return u
	}
	ctxs := u.ctx.([]interface{})
	def := timelineDef{name: id, by: make([]string, len(ctxs))}
	for i, ctx := range ctxs {
		def.by[i] = ctx.(string)
	}

    return pRune(u, ')').with(def)
}

func pIdent(t token) token {
    var start int
    for start = 0; start < len(t.runes) && unicode.IsSpace(t.runes[start]); start++ { }
    var stop int
	for stop = start; stop < len(t.runes); stop++ {
		c := t.runes[stop]
		if c != '_' && !unicode.IsLetter(c) && !unicode.IsDigit(c) {
			break
		}
	}
    if start == stop {
        return t.fail("expected identifier")
    }
	return t.advance(stop).with(string(t.runes[start:stop]))
}

func pComma(t token) token {
    return pRune(t, ',')
}

func pRune(t token, c rune) token {
    for len(t.runes) > 0 && unicode.IsSpace(t.runes[0]) {
        t.runes = t.runes[1:]
    }
    if len(t.runes) == 0 {
        t.eof = true
        return t.failf("tag terminated early, expected %c", c)
    }
    if t.runes[0] != c {
        return t.failf("expected '%c', got '%c'", c, t.runes[0])
    }
    return t.advance(1).with(c)
}

func pEof(t token) token {
	if t.eof {
		return t.fail("eof")
	}
	u := t
	u.eof = true
	return u
}

func list(rep, sep parser, t token) token {
	ctxs := make([]interface{}, 0)
	u := rep(t)
	if u.err != nil {
		return t.with(ctxs)
	}
	t = u
	for t.err == nil {
		ctxs = append(ctxs, t.ctx)
		if u = sep(t); u.err != nil {
			break
		}
		t = u
		if t = rep(t); t.err != nil {
			return t
		}
	}
	return t.with(ctxs)
}
