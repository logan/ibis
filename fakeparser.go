package ibis

import "errors"
import "fmt"
import "strconv"
import "strings"
import "unicode"

import "github.com/gocql/gocql"

type statement struct {
	text string
	cmd  command
}

func newStatement(text string) *statement {
	return &statement{text: text}
}

func (s *statement) Compile() error {
	stmt := &pStmt{text: s.text}
	t := pStatement(pToken{stmt: stmt, runes: []rune(s.text)})
	if t.err != nil {
		o := t.offset
		pad := make([]byte, o)
		for i := 0; i < o; i++ {
			pad[i] = 32
		}
		return errors.New(fmt.Sprintf("%s\n%s\n%s^\n", t.err, s.text, string(pad)))
	}
	s.cmd = t.ctx.(command)
	return nil
}

func (s *statement) Execute(ks *fakeKeyspace, params ...interface{}) (resultSet, error) {
	bind := make(valueList, len(params))
	for i, param := range params {
		bind[i] = LiteralValue(param)
	}
	rs, err := s.cmd.Execute(ks, bind)
	return rs, err
}

type pStmt struct {
	text    string
	numVars int
}

type pToken struct {
	stmt   *pStmt
	runes  []rune
	offset int
	ctx    interface{}
	err    error
}

func (t pToken) eof() bool { return len(t.runes) == 0 }

func (t pToken) fail(vals ...interface{}) pToken {
	newtok := t
	newtok.err = errors.New(fmt.Sprintf("%d: %s", t.offset, fmt.Sprint(vals...)))
	return newtok
}

func (t pToken) failf(format string, args ...interface{}) pToken {
	return t.fail(fmt.Sprintf(format, args...))
}

func (t pToken) minus(u pToken) string {
	if t.offset <= u.offset {
		return ""
	}
	return string(u.runes[:t.offset-u.offset])
}

func (t pToken) advance(n int) pToken {
	newtok := t
	if n > len(t.runes) {
		n = len(t.runes)
	}
	newtok.runes = newtok.runes[n:]
	newtok.offset += n
	return newtok
}

func (t pToken) with(ctx interface{}) pToken {
	t.ctx = ctx
	return t
}

type _parser func(pToken) pToken

func parseWith(text string, grammar _parser) pToken {
	s := &pStmt{text: text}
	t := pToken{stmt: s, runes: []rune(text)}
	return grammar(t)
}

func gRequire(p _parser, ctx interface{}) _parser {
	return func(t pToken) pToken {
		u := p(t)
		if u.err != nil {
			return u
		}
		if u.ctx != ctx {
			return t.failf("expected %v, got %v", ctx, u.ctx)
		}
		return u
	}
}

func gList(p _parser, sep _parser) _parser {
	ctx := make([]interface{}, 0)
	return func(t pToken) pToken {
		if t = p(t); t.err != nil {
			return t
		}
		ctx = append(ctx, t.ctx)
		for !t.eof() && t.err == nil {
			if sep != nil {
				if u := sep(t); u.err == nil {
					t = u
				} else {
					break
				}
			}
			if u := p(t); u.err == nil {
				ctx = append(ctx, u.ctx)
				t = u
			} else {
				if sep != nil {
					t = u
				}
				break
			}
		}
		return t.with(ctx)
	}
}

func pStatement(t pToken) pToken {
	u := pTerm(t)
	keyword, ok := u.ctx.(termKeyword)
	if !ok {
		return t.fail("expected command")
	}
	switch keyword {
	case "alter":
		t = pAlter(u)
	case "create":
		t = pCreate(u)
	case "delete":
		t = pDelete(u)
	case "drop":
		t = pDrop(u)
	case "insert":
		t = pInsert(u)
	case "select":
		t = pSelect(u)
	case "update":
		t = pUpdate(u)
	case "use":
		t = pUse(u)
	default:
		return t.fail("invalid command: ", keyword)
	}
	if t.err == nil && !t.eof() {
		return t.fail("trailing text after complete statement")
	}
	return t
}

func pUse(t pToken) pToken {
	var cmd useCommand
	if t = pTermId(t); t.err != nil {
		return t
	}
	cmd.identifier = string(t.ctx.(termId))
	return t.with(&cmd)
}

func pCreate(t pToken) pToken {
	u := pTerm(t)
	kw, _ := u.ctx.(termKeyword)
	switch kw {
	case "keyspace":
		return pCreateKeyspace(u)
	case "table", "columnfamily":
		return pCreateTable(u)
	default:
		return t.fail("expected KEYSPACE or TABLE")
	}
}

func pCreateKeyspace(t pToken) pToken {
	var cmd createKeyspaceCommand
	if u := pIfNotExists(t); u.err == nil {
		t = u
	} else {
		cmd.strict = true
	}
	if t = pTermId(t); t.err != nil {
		return t
	}
	cmd.identifier = string(t.ctx.(termId))
	if t = pWithOptions(t); t.err != nil {
		return t
	}
	if t.ctx != nil {
		cmd.options = t.ctx.(optionMap)
	}
	return t.with(&cmd)
}

func pCreateTable(t pToken) pToken {
	var cmd createTableCommand
	if u := pIfNotExists(t); u.err == nil {
		t = u
	} else {
		cmd.strict = true
	}
	if t = pTermId(t); t.err != nil {
		return t
	}
	cmd.identifier = string(t.ctx.(termId))
	if t = gRequire(pTerm, termSymbol("("))(t); t.err != nil {
		return t
	}
	if t = gList(pColumnDef, pTermComma)(t); t.err != nil {
		return t
	}
	ctxs := t.ctx.([]interface{})
	for _, ctx := range ctxs {
		cdef := ctx.(*ctxColumnDef)
		if cdef.keys != nil {
			if cmd.key != nil {
				return t.fail("multiple primary key definitions")
			}
			cmd.key = cdef.keys
		}
		if cdef.colName != "" {
			cmd.colnames = append(cmd.colnames, cdef.colName)
			cmd.coltypes = append(cmd.coltypes, cdef.colType)
		}
	}
	if t = gRequire(pTerm, termSymbol(")"))(t); t.err != nil {
		return t
	}
	if t = pWithOptions(t); t.err != nil {
		return t
	}
	if t.ctx != nil {
		cmd.options = t.ctx.(optionMap)
	}
	return t.with(&cmd)
}

type ctxColumnDef struct {
	colName string
	colType *gocql.TypeInfo
	keys    []string
}

func pColumnDef(t pToken) pToken {
	cdef := &ctxColumnDef{}
	if u := gRequire(pTerm, termKeyword("primary"))(t); u.err == nil {
		if u = gRequire(pTerm, termKeyword("key"))(u); u.err != nil {
			return u
		}
		if u = gRequire(pTerm, termSymbol("("))(u); u.err != nil {
			return u
		}
		if u = pTermIdList(u); u.err != nil {
			return u
		}
		ctxs := u.ctx.([]interface{})
		cdef.keys = make([]string, len(ctxs))
		for i, ctx := range ctxs {
			cdef.keys[i] = string(ctx.(termId))
		}
		if u = gRequire(pTerm, termSymbol(")"))(u); u.err != nil {
			return u
		}
		return u.with(cdef)
	}
	if t = pTermId(t); t.err != nil {
		return t
	}
	cdef.colName = string(t.ctx.(termId))
	if t = pDataType(t); t.err != nil {
		return t
	}
	cdef.colType = t.ctx.(*gocql.TypeInfo)
	if u := gRequire(pTerm, termKeyword("primary"))(t); u.err == nil {
		if u = gRequire(pTerm, termKeyword("key"))(u); u.err != nil {
			return u
		}
		cdef.keys = []string{cdef.colName}
		t = u
	}
	return t.with(cdef)
}

func pWithOptions(t pToken) pToken {
	u := gRequire(pTerm, termKeyword("with"))(t)
	if u.err != nil {
		return t.advance(0).with(nil)
	}
	t = u
	if t = gList(pOption, gRequire(pTerm, termKeyword("and")))(t); t.err != nil {
		return t
	}
	options := make(optionMap)
	ctxs := t.ctx.([]interface{})
	for _, ctx := range ctxs {
		opt := ctx.(*ctxOption)
		options[opt.key] = opt.val
	}
	return t.with(options)
}

type ctxOption struct {
	key string
	val pval
}

func pOption(t pToken) pToken {
	if t = pTermId(t); t.err != nil {
		return t
	}
	key := string(t.ctx.(termId))
	if t = gRequire(pTerm, termSymbol("="))(t); t.err != nil {
		return t
	}
	if t = pValue(t); t.err != nil {
		return t
	}
	return t.with(&ctxOption{key, t.ctx.(pval)})
}

func pDrop(t pToken) pToken {
	var cmd dropCommand
	u := pTerm(t)
	kw, _ := u.ctx.(termKeyword)
	if kw != "keyspace" && kw != "table" {
		return t.fail("expected KEYSPACE or TABLE")
	}
	cmd.dropType = string(kw)
	t = u
	if u = pIfExists(t); u.err == nil {
		t = u
	} else {
		cmd.strict = true
	}
	if t = pTermId(t); t.err != nil {
		return t
	}
	cmd.identifier = string(t.ctx.(termId))
	return t.with(&cmd)
}

func pAlter(t pToken) pToken {
	var cmd alterCommand
	// TODO: support ALTER KEYSPACE
	if t = gRequire(pTerm, termKeyword("table"))(t); t.err != nil {
		return t
	}
	if t = pTermId(t); t.err != nil {
		return t
	}
	cmd.table = string(t.ctx.(termId))
	u := pTerm(t)
	kw, _ := u.ctx.(termKeyword)
	switch kw {
	case "with":
		if t = pWithOptions(t); t.err != nil {
			return t
		}
		cmd.options = t.ctx.(optionMap)
	case "add", "alter":
		if t = pTermId(u); t.err != nil {
			return t
		}
		if kw == "add" {
			cmd.add = string(t.ctx.(termId))
		} else {
			cmd.alter = string(t.ctx.(termId))
			if t = gRequire(pTerm, termKeyword("type"))(t); t.err != nil {
				return t
			}
		}
		if t = pDataType(t); t.err != nil {
			return t
		}
		cmd.coltype = t.ctx.(*gocql.TypeInfo)
	case "drop":
		if t = pTermId(u); t.err != nil {
			return t
		}
		cmd.drop = string(t.ctx.(termId))
	default:
		return t.fail("expected ADD, ALTER, DROP, or WITH")
	}
	return t.with(&cmd)
}

func pInsert(t pToken) pToken {
	if t = gRequire(pTerm, termKeyword("into"))(t); t.err != nil {
		return t
	}
	if t = pTermId(t); t.err != nil {
		return t
	}
	id := string(t.ctx.(termId))
	if t = gRequire(pTerm, termSymbol("("))(t); t.err != nil {
		return t
	}
	if t = pTermIdList(t); t.err != nil {
		return t
	}
	ctxs := t.ctx.([]interface{})
	keys := make([]string, len(ctxs))
	for i, ctx := range ctxs {
		keys[i] = string(ctx.(termId))
	}
	if t = gRequire(pTerm, termSymbol(")"))(t); t.err != nil {
		return t
	}
	if t = gRequire(pTerm, termKeyword("values"))(t); t.err != nil {
		return t
	}
	if t = gRequire(pTerm, termSymbol("("))(t); t.err != nil {
		return t
	}
	if t = gList(pValue, pTermComma)(t); t.err != nil {
		return t
	}
	ctxs = t.ctx.([]interface{})
	vals := make([]pval, len(ctxs))
	for i, ctx := range ctxs {
		vals[i] = ctx.(pval)
	}
	if t = gRequire(pTerm, termSymbol(")"))(t); t.err != nil {
		return t
	}
	cas := false
	u := pIfNotExists(t)
	if u.err == nil {
		t = u
		cas = true
	}
	return t.with(&insertCommand{table: id, keys: keys, values: vals, cas: cas})
}

func pIfExists(t pToken) pToken {
	if t = gRequire(pTerm, termKeyword("if"))(t); t.err != nil {
		return t
	}
	return gRequire(pTerm, termKeyword("exists"))(t)
}

func pIfNotExists(t pToken) pToken {
	if t = gRequire(pTerm, termKeyword("if"))(t); t.err != nil {
		return t
	}
	if t = gRequire(pTerm, termKeyword("not"))(t); t.err != nil {
		return t
	}
	return gRequire(pTerm, termKeyword("exists"))(t)
}

func pUpdate(t pToken) pToken {
	var cmd updateCommand
	// TODO: USING ... and IF ...
	if t = pTermId(t); t.err != nil {
		return t
	}
	cmd.table = string(t.ctx.(termId))
	if t = gRequire(pTerm, termKeyword("set"))(t); t.err != nil {
		return t
	}
	if t = gList(pAssignOrEquals, pTermComma)(t); t.err != nil {
		return t
	}
	cmd.set = make(map[string]pval)
	for _, ctx := range t.ctx.([]interface{}) {
		a := ctx.(*ctxKeyValue)
		cmd.set[a.id] = a.val
	}
	if t = gRequire(pTerm, termKeyword("where"))(t); t.err != nil {
		return t
	}
	if t = gList(pAssignOrEquals, pTermAnd)(t); t.err != nil {
		return t
	}
	cmd.key = make(map[string]pval)
	for _, ctx := range t.ctx.([]interface{}) {
		w := ctx.(*ctxKeyValue)
		cmd.key[w.id] = w.val
	}
	return t.with(&cmd)
}

func pDelete(t pToken) pToken {
	var cmd deleteCommand
	if t = gRequire(pTerm, termKeyword("from"))(t); t.err != nil {
		return t
	}
	if t = pTermId(t); t.err != nil {
		return t
	}
	cmd.table = string(t.ctx.(termId))
	if t = gRequire(pTerm, termKeyword("where"))(t); t.err != nil {
		return t
	}
	if t = gList(pAssignOrEquals, pTermAnd)(t); t.err != nil {
		return t
	}
	cmd.key = make(map[string]pval)
	for _, ctx := range t.ctx.([]interface{}) {
		w := ctx.(*ctxKeyValue)
		cmd.key[w.id] = w.val
	}
	return t.with(&cmd)
}

func pSelect(t pToken) pToken {
	// TODO: DISTINCT, functions (except for a lone COUNT(*) or COUNT(1)), AS, WRITETIME, TTL, IN
	var cmd selectCommand
	if t = pSelectList(t); t.err != nil {
		return t
	}
	cmd.cols = t.ctx.([]string)
	if t = gRequire(pTerm, termKeyword("from"))(t); t.err != nil {
		return t
	}
	if t = pTermId(t); t.err != nil {
		return t
	}
	cmd.table = string(t.ctx.(termId))

	if u := gRequire(pTerm, termKeyword("where"))(t); u.err == nil {
		t = u
		if t = gList(pComparison, pTermAnd)(t); t.err != nil {
			return t
		}
		ctxs := t.ctx.([]interface{})
		cmd.where = make([]comparison, len(ctxs))
		for i, ctx := range ctxs {
			cmd.where[i] = ctx.(comparison)
		}
	}

	if u := gRequire(pTerm, termKeyword("order"))(t); u.err == nil {
		t = u
		if t = gRequire(pTerm, termKeyword("by"))(t); t.err != nil {
			return t
		}
		if t = gList(pOrderSpec, pTermComma)(t); t.err != nil {
			return t
		}
		ctxs := t.ctx.([]interface{})
		cmd.order = make([]order, len(ctxs))
		for i, ctx := range ctxs {
			cmd.order[i] = ctx.(order)
		}
	}

	if u := gRequire(pTerm, termKeyword("limit"))(t); u.err == nil {
		t = u
		u = pTerm(t)
		limit, ok := u.ctx.(termNumber)
		if !ok {
			return t.fail("expected number")
		}
		t = u
		cmd.limit = int(limit)
	}
	return t.with(&cmd)
}

func pSelectList(t pToken) pToken {
	u := pTerm(t)
	switch v := u.ctx.(type) {
	case termSymbol:
		if v == "*" {
			return u.with([]string{"*"})
		}
	case termKeyword:
		if v == "count" {
			t = u
			if t = gRequire(pTerm, termSymbol("("))(t); t.err != nil {
				return t
			}
			u = pTerm(t)
			switch v := u.ctx.(type) {
			case termNumber:
				t = u
			case termSymbol:
				if v != "*" {
					return t.fail("expected * or 1")
				}
				t = u
			default:
				t.fail("expected * or 1")
			}
			if t = gRequire(pTerm, termSymbol(")"))(t); t.err != nil {
				return t
			}
			return t.with([]string{"count(*)"})
		}
	case termId:
		if t = pTermIdList(t); t.err != nil {
			return t
		}
		ctxs := t.ctx.([]interface{})
		ids := make([]string, len(ctxs))
		for i, ctx := range ctxs {
			ids[i] = string(ctx.(termId))
		}
		return t.with(ids)
	}
	return t.fail("expected *, COUNT, or identifier")
}

type ctxKeyValue struct {
	id  string
	val pval
}

func pAssignOrEquals(t pToken) pToken {
	var ctx ctxKeyValue
	if t = pTermId(t); t.err != nil {
		return t
	}
	ctx.id = string(t.ctx.(termId))
	if t = gRequire(pTerm, termSymbol("="))(t); t.err != nil {
		return t
	}
	if t = pValue(t); t.err != nil {
		return t
	}
	ctx.val = t.ctx.(pval)
	return t.with(&ctx)
}

func pComparison(t pToken) pToken {
	// TODO: IN, TOKEN
	var cmp comparison
	if t = pTermId(t); t.err != nil {
		return t
	}
	cmp.col = string(t.ctx.(termId))
	u := pTerm(t)
	sym, ok := u.ctx.(termSymbol)
	if !ok {
		return t.fail("expected comparison operator")
	}
	switch sym {
	case "=", "<", "<=", ">", ">=":
		cmp.op = string(sym)
	default:
		return t.fail("expected comparison operator")
	}
	if t = pValue(u); t.err != nil {
		return t
	}
	cmp.val = t.ctx.(pval)
	return t.with(cmp)
}

func pOrderSpec(t pToken) pToken {
	var ord order
	if t = pTermId(t); t.err != nil {
		return t
	}
	ord.col = string(t.ctx.(termId))
	u := pTerm(t)
	if kw, ok := u.ctx.(termKeyword); ok {
		switch kw {
		case "asc":
			ord.dir = asc
		case "desc":
			ord.dir = desc
		default:
			return t.with(ord)
		}
		t = u
	}
	return t.with(ord)
}

func pDataType(t pToken) pToken {
	u := pTerm(t)
	kw, ok := u.ctx.(termKeyword)
	if !ok {
		return t.fail("expected column type")
	}
	ti, ok := typeInfoMap[string(kw)]
	if !ok {
		return t.fail("expected column type")
	}
	return u.with(ti)
}

type termVar int
type termId string
type termKeyword string
type termSymbol string
type termString string
type termNumber int

func pTermIdList(t pToken) pToken {
	return gList(pTermId, pTermComma)(t)
}

func pTermId(t pToken) pToken {
	u := pTerm(t)
	if _, ok := u.ctx.(termId); ok {
		return u
	}
	return t.fail("expected identifier")
}

func pTermComma(t pToken) pToken {
	if u := pTerm(t); u.err == nil {
		switch x := u.ctx.(type) {
		case termSymbol:
			if x == "," {
				return u
			}
		}
	}
	return t.fail("expected comma")
}

func pTermAnd(t pToken) pToken {
	if u := pTerm(t); u.err == nil {
		switch x := u.ctx.(type) {
		case termKeyword:
			if x == "and" {
				return u
			}
		}
	}
	return t.fail("expected AND")
}

func pValue(t pToken) pToken {
	if t.eof() {
		return t.fail("expected value, got end of statement")
	}
	u := pTerm(t)
	if u.err == nil {
		switch v := u.ctx.(type) {
		case termVar:
			return u.with(pval{VarIndex: int(v)})
		case termString:
			return u.with(pval{Value: LiteralValue(string(v))})
		case termNumber:
			return u.with(pval{Value: LiteralValue(int(v))})
		case termSymbol:
			// TODO: add collection values to context
			switch v {
			case "{":
				if u = pMapOrSetValue(u); u.err != nil {
					return u
				}
				if u = gRequire(pTerm, termSymbol("}"))(u); u.err != nil {
					return u
				}
				return u.with(pval{Value: LiteralValue("")})
			case "[":
				if u = pListValue(u); u.err != nil {
					return u
				}
				if u = gRequire(pTerm, termSymbol("]"))(u); u.err != nil {
					return u
				}
				return u.with(pval{Value: LiteralValue("")})
			}
		}
	}
	return t.fail("expected value")
}

func pMapOrSetValue(t pToken) pToken {
	u := pValue(t)
	if u.err != nil {
		return u
	}
	if u = gRequire(pTerm, termSymbol(":"))(u); u.err == nil {
		return pMapValue(t)
	} else {
		return pSetValue(t)
	}
}

func pListValue(t pToken) pToken {
	return gList(pValue, pTermComma)(t)
}

func pSetValue(t pToken) pToken {
	return gList(pValue, pTermComma)(t)
}

func pMapValue(t pToken) pToken {
	return gList(pMapEntry, pTermComma)(t)
}

func pMapEntry(t pToken) pToken {
	if t = pValue(t); t.err != nil {
		return t
	}
	if t = gRequire(pTerm, termSymbol(":"))(t); t.err != nil {
		return t
	}
	return pValue(t)
}

func pTerm(t pToken) pToken {
	t = pSkipSpace(t)
	if t.eof() {
		return t
	}
	first := t.runes[0]
	if first == '?' {
		v := termVar(t.stmt.numVars)
		t.stmt.numVars++
		return pSkipSpace(t.advance(1)).with(v)
	} else if first == '\'' {
		return pSkipSpace(pStringLiteral(t))
	} else if unicode.IsDigit(first) {
		return pSkipSpace(pNumberLiteral(t))
	} else if first == '_' || unicode.IsLetter(first) {
		r := pSkipAlphanumeric(t)
		id := strings.ToLower(r.minus(t))
		s := pSkipSpace(r)
		if isKeyword(id) {
			return s.with(termKeyword(id))
		}
		return s.with(termId(id))
	} else {
		return pSkipSpace(pSymbol(t))
	}
}

func pStringLiteral(t pToken) pToken {
	backslash := false
	parts := make([]string, 0, 1)
	last := 1
	var i int
	for i = 1; i < len(t.runes); i++ {
		c := t.runes[i]
		if backslash {
			backslash = false
			last = i
		} else if c == '\\' {
			parts = append(parts, string(t.runes[last:i]))
			backslash = true
		} else if c == '\'' {
			break
		}
	}
	if i == len(t.runes) {
		return t.fail("unterminated string constant")
	}
	parts = append(parts, string(t.runes[last:i]))
	return t.advance(i + 1).with(termString(strings.Join(parts, "")))
}

func pNumberLiteral(t pToken) pToken {
	var i int
	for i = 0; i < len(t.runes) && unicode.IsDigit(t.runes[i]); i++ {
	}
	x, err := strconv.Atoi(string(t.runes[:i]))
	if err != nil {
		return t.fail(err)
	}
	return t.advance(i).with(termNumber(x))
}

func pSkipSpace(t pToken) pToken {
	for i := 0; t.err == nil && i < len(t.runes); i++ {
		if !unicode.IsSpace(t.runes[i]) {
			return t.advance(i)
		}
	}
	return pSkipAll(t)
}

func pSkipAlphanumeric(t pToken) pToken {
	for i := 0; i < len(t.runes); i++ {
		next := t.runes[i]
		if next != '.' && next != '_' && !unicode.IsLetter(next) && !unicode.IsDigit(next) {
			return t.advance(i)
		}
	}
	return pSkipAll(t)
}

func pSkipAll(t pToken) pToken {
	newtok := t
	newtok.offset += len(t.runes)
	newtok.runes = []rune{}
	return newtok
}

func isKeyword(id string) bool {
	switch id {
	case "use":
		return true
	case "create":
		return true
	case "drop":
		return true
	case "keyspace":
		return true
	case "if":
		return true
	case "exists":
		return true
	case "not":
		return true
	case "with":
		return true
	case "and":
		return true
	case "table":
		return true
	case "columnfamily":
		return true
	case "primary":
		return true
	case "key":
		return true
	case "alter":
		return true
	case "add":
		return true
	case "insert":
		return true
	case "into":
		return true
	case "values":
		return true
	case "update":
		return true
	case "set":
		return true
	case "where":
		return true
	case "delete":
		return true
	case "from":
		return true
	case "select":
		return true
	case "count":
		return true
	case "order":
		return true
	case "by":
		return true
	case "asc":
		return true
	case "desc":
		return true
	case "limit":
		return true
	case "type":
		return true
	case "varchar":
		return true
	case "boolean":
		return true
	case "bigint":
		return true
	case "double":
		return true
	case "timestamp":
		return true
	case "blob":
		return true
	case "timeuuid":
		return true
	default:
		return false
	}
}

func pSymbol(t pToken) pToken {
	switch t.runes[0] {
	case '<', '>':
		if len(t.runes) > 1 && t.runes[1] == '=' {
			return t.advance(2).with(termSymbol(string(t.runes[:2])))
		}
		return t.advance(1).with(termSymbol(string(t.runes[:1])))
	case '=', '{', '}', '[', ']', '(', ')', ':', ',', '*', '\'', '"':
		return t.advance(1).with(termSymbol(string(t.runes[:1])))
	default:
		return t.failf("don't know how to handle character '%c' (%#v)", t.runes[0], t.runes[0])
	}
}
