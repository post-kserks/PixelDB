package parser

import (
	"fmt"
	"strconv"
	"strings"

	"pixeldb/internal/lexer"
)

// Parse parses one SQL statement terminated by ';'.
func Parse(sql string) (Statement, error) {
	if strings.TrimSpace(sql) == "" {
		return nil, fmt.Errorf("syntax error: empty query")
	}

	l := lexer.New(sql)
	tokens := make([]lexer.Token, 0, 64)
	for {
		tok := l.NextToken()
		if tok.Type == lexer.TOKEN_ILLEGAL {
			return nil, fmt.Errorf("syntax error at line %d, col %d: illegal token '%s'", tok.Line, tok.Col, tok.Literal)
		}
		tokens = append(tokens, tok)
		if tok.Type == lexer.TOKEN_EOF {
			break
		}
	}

	p := &sqlParser{tokens: tokens}
	stmt, err := p.parseStatement()
	if err != nil {
		return nil, err
	}

	if p.current().Type != lexer.TOKEN_SEMICOLON {
		if p.current().Type == lexer.TOKEN_EOF {
			return nil, fmt.Errorf("syntax error: unexpected end of input, expected ';'")
		}
		return nil, p.expectedError("';'", p.current())
	}
	p.advance()

	if p.current().Type != lexer.TOKEN_EOF {
		return nil, p.syntaxError(p.current(), "unexpected token after ';'")
	}

	return stmt, nil
}

type sqlParser struct {
	tokens []lexer.Token
	pos    int
}

func (p *sqlParser) parseStatement() (Statement, error) {
	switch p.current().Type {
	case lexer.TOKEN_CREATE:
		return p.parseCreate()
	case lexer.TOKEN_DROP:
		return p.parseDrop()
	case lexer.TOKEN_USE:
		return p.parseUse()
	case lexer.TOKEN_SELECT:
		return p.parseSelect()
	case lexer.TOKEN_INSERT:
		return p.parseInsert()
	case lexer.TOKEN_UPDATE:
		return p.parseUpdate()
	case lexer.TOKEN_DELETE:
		return p.parseDelete()
	default:
		return nil, p.expectedError("a statement", p.current())
	}
}

func (p *sqlParser) parseCreate() (Statement, error) {
	p.advance() // CREATE
	switch p.current().Type {
	case lexer.TOKEN_DATABASE:
		p.advance()
		name, err := p.consumeIdent("database name")
		if err != nil {
			return nil, err
		}
		return &CreateDatabaseStatement{DatabaseName: name}, nil
	case lexer.TOKEN_TABLE:
		return p.parseCreateTable()
	case lexer.TOKEN_INDEX:
		return p.parseCreateIndex()
	default:
		return nil, p.expectedError("DATABASE, TABLE, or INDEX", p.current())
	}
}

func (p *sqlParser) parseCreateTable() (Statement, error) {
	p.advance() // TABLE
	tableName, err := p.consumeIdent("table name")
	if err != nil {
		return nil, err
	}

	if err := p.consume(lexer.TOKEN_LPAREN, "'('"); err != nil {
		return nil, err
	}

	columns := make([]ColumnDef, 0, 8)
	for {
		colName, err := p.consumeIdent("column name")
		if err != nil {
			return nil, err
		}

		dataType, varcharLen, err := p.parseColumnType()
		if err != nil {
			return nil, err
		}

		columns = append(columns, ColumnDef{Name: colName, DataType: dataType, VarcharLen: varcharLen})

		if p.current().Type == lexer.TOKEN_COMMA {
			p.advance()
			continue
		}
		if p.current().Type == lexer.TOKEN_RPAREN {
			p.advance()
			break
		}
		return nil, p.expectedError("',' or ')'", p.current())
	}

	if len(columns) == 0 {
		return nil, fmt.Errorf("syntax error: CREATE TABLE requires at least one column")
	}

	return &CreateTableStatement{TableName: tableName, Columns: columns}, nil
}

func (p *sqlParser) parseCreateIndex() (Statement, error) {
	p.advance() // INDEX

	indexName, err := p.consumeIdent("index name")
	if err != nil {
		return nil, err
	}
	if err := p.consume(lexer.TOKEN_ON, "ON"); err != nil {
		return nil, err
	}
	tableName, err := p.consumeIdent("table name")
	if err != nil {
		return nil, err
	}
	if err := p.consume(lexer.TOKEN_LPAREN, "'('"); err != nil {
		return nil, err
	}
	columnName, err := p.consumeIdent("column name")
	if err != nil {
		return nil, err
	}
	if err := p.consume(lexer.TOKEN_RPAREN, "')'"); err != nil {
		return nil, err
	}

	return &CreateIndexStatement{
		IndexName:  indexName,
		TableName:  tableName,
		ColumnName: columnName,
	}, nil
}

func (p *sqlParser) parseColumnType() (string, int, error) {
	tok := p.current()
	switch tok.Type {
	case lexer.TOKEN_INT:
		p.advance()
		return "INT", 0, nil
	case lexer.TOKEN_FLOAT_TYPE:
		p.advance()
		return "FLOAT", 0, nil
	case lexer.TOKEN_BOOL:
		p.advance()
		return "BOOL", 0, nil
	case lexer.TOKEN_TEXT:
		p.advance()
		return "TEXT", 0, nil
	case lexer.TOKEN_VARCHAR:
		p.advance()
		if err := p.consume(lexer.TOKEN_LPAREN, "'('"); err != nil {
			return "", 0, err
		}
		sizeTok := p.current()
		if sizeTok.Type != lexer.TOKEN_INT_LIT {
			return "", 0, p.expectedError("VARCHAR length", sizeTok)
		}
		size, err := strconv.Atoi(sizeTok.Literal)
		if err != nil || size <= 0 {
			return "", 0, p.syntaxError(sizeTok, "VARCHAR length must be a positive integer")
		}
		p.advance()
		if err := p.consume(lexer.TOKEN_RPAREN, "')'"); err != nil {
			return "", 0, err
		}
		return "VARCHAR", size, nil
	case lexer.TOKEN_IDENT:
		return "", 0, fmt.Errorf("unknown data type '%s' at line %d, col %d", tok.Literal, tok.Line, tok.Col)
	default:
		return "", 0, p.expectedError("data type", tok)
	}
}

func (p *sqlParser) parseDrop() (Statement, error) {
	p.advance() // DROP
	switch p.current().Type {
	case lexer.TOKEN_DATABASE:
		p.advance()
		name, err := p.consumeIdent("database name")
		if err != nil {
			return nil, err
		}
		return &DropDatabaseStatement{DatabaseName: name}, nil
	case lexer.TOKEN_TABLE:
		p.advance()
		name, err := p.consumeIdent("table name")
		if err != nil {
			return nil, err
		}
		return &DropTableStatement{TableName: name}, nil
	case lexer.TOKEN_INDEX:
		return p.parseDropIndex()
	default:
		return nil, p.expectedError("DATABASE, TABLE, or INDEX", p.current())
	}
}

func (p *sqlParser) parseDropIndex() (Statement, error) {
	p.advance() // INDEX
	indexName, err := p.consumeIdent("index name")
	if err != nil {
		return nil, err
	}
	if err := p.consume(lexer.TOKEN_ON, "ON"); err != nil {
		return nil, err
	}
	tableName, err := p.consumeIdent("table name")
	if err != nil {
		return nil, err
	}
	return &DropIndexStatement{
		IndexName: indexName,
		TableName: tableName,
	}, nil
}

func (p *sqlParser) parseUse() (Statement, error) {
	p.advance() // USE
	name, err := p.consumeIdent("database name")
	if err != nil {
		return nil, err
	}
	return &UseDatabaseStatement{DatabaseName: name}, nil
}

func (p *sqlParser) parseSelect() (Statement, error) {
	p.advance() // SELECT

	items, err := p.parseSelectItems()
	if err != nil {
		return nil, err
	}

	if err := p.consume(lexer.TOKEN_FROM, "FROM"); err != nil {
		return nil, err
	}

	tableName, err := p.consumeIdent("table name")
	if err != nil {
		return nil, err
	}

	var where Expression
	if p.current().Type == lexer.TOKEN_WHERE {
		p.advance()
		where, err = p.parseExpression()
		if err != nil {
			return nil, err
		}
	}

	groupBy := make([]string, 0, 2)
	if p.current().Type == lexer.TOKEN_GROUP {
		p.advance()
		if err := p.consume(lexer.TOKEN_BY, "BY"); err != nil {
			return nil, err
		}
		groupBy, err = p.parseIdentifierList("group by column")
		if err != nil {
			return nil, err
		}
	}

	orderBy := make([]OrderByItem, 0, 2)
	if p.current().Type == lexer.TOKEN_ORDER {
		p.advance()
		if err := p.consume(lexer.TOKEN_BY, "BY"); err != nil {
			return nil, err
		}
		orderBy, err = p.parseOrderByItems()
		if err != nil {
			return nil, err
		}
	}

	var limit *LimitClause
	if p.current().Type == lexer.TOKEN_LIMIT {
		limit, err = p.parseLimitClause()
		if err != nil {
			return nil, err
		}
	} else if p.current().Type == lexer.TOKEN_OFFSET {
		return nil, p.expectedError("LIMIT before OFFSET", p.current())
	}

	return &SelectStatement{
		Items:     items,
		TableName: tableName,
		Where:     where,
		GroupBy:   groupBy,
		OrderBy:   orderBy,
		Limit:     limit,
	}, nil
}

func (p *sqlParser) parseSelectItems() ([]SelectItem, error) {
	if p.current().Type == lexer.TOKEN_STAR {
		p.advance()
		return []SelectItem{{Type: "all", ColumnName: "*"}}, nil
	}

	items := make([]SelectItem, 0, 4)
	for {
		item, err := p.parseSelectItem()
		if err != nil {
			return nil, err
		}
		items = append(items, item)
		if p.current().Type != lexer.TOKEN_COMMA {
			break
		}
		p.advance()
	}
	return items, nil
}

func (p *sqlParser) parseSelectItem() (SelectItem, error) {
	switch p.current().Type {
	case lexer.TOKEN_COUNT, lexer.TOKEN_SUM, lexer.TOKEN_AVG, lexer.TOKEN_MIN, lexer.TOKEN_MAX:
		return p.parseAggregateSelectItem()
	case lexer.TOKEN_IDENT:
		col := p.current().Literal
		p.advance()
		item := SelectItem{
			Type:       "column",
			ColumnName: col,
		}
		if strings.EqualFold(col, "_score") {
			item.Type = "score"
		}
		alias, err := p.parseOptionalAlias()
		if err != nil {
			return SelectItem{}, err
		}
		item.Alias = alias
		return item, nil
	default:
		return SelectItem{}, p.expectedError("column, _score, or aggregate expression", p.current())
	}
}

func (p *sqlParser) parseAggregateSelectItem() (SelectItem, error) {
	funcTok := p.current()
	p.advance()

	if err := p.consume(lexer.TOKEN_LPAREN, "'('"); err != nil {
		return SelectItem{}, err
	}

	column := ""
	if funcTok.Type == lexer.TOKEN_COUNT && p.current().Type == lexer.TOKEN_STAR {
		column = "*"
		p.advance()
	} else {
		col, err := p.consumeIdent("aggregate column")
		if err != nil {
			return SelectItem{}, err
		}
		column = col
	}

	if err := p.consume(lexer.TOKEN_RPAREN, "')'"); err != nil {
		return SelectItem{}, err
	}

	alias, err := p.parseOptionalAlias()
	if err != nil {
		return SelectItem{}, err
	}

	return SelectItem{
		Type:       "agg",
		ColumnName: column,
		AggFunc:    strings.ToUpper(funcTok.Literal),
		Alias:      alias,
	}, nil
}

func (p *sqlParser) parseOptionalAlias() (string, error) {
	if p.current().Type != lexer.TOKEN_AS {
		return "", nil
	}
	p.advance()
	return p.consumeIdent("alias")
}

func (p *sqlParser) parseOrderByItems() ([]OrderByItem, error) {
	items := make([]OrderByItem, 0, 2)
	for {
		col, err := p.consumeIdent("order by column")
		if err != nil {
			return nil, err
		}
		item := OrderByItem{Column: col}
		if p.current().Type == lexer.TOKEN_ASC {
			p.advance()
		} else if p.current().Type == lexer.TOKEN_DESC {
			item.Desc = true
			p.advance()
		}
		items = append(items, item)

		if p.current().Type != lexer.TOKEN_COMMA {
			break
		}
		p.advance()
	}
	return items, nil
}

func (p *sqlParser) parseLimitClause() (*LimitClause, error) {
	if err := p.consume(lexer.TOKEN_LIMIT, "LIMIT"); err != nil {
		return nil, err
	}
	count, err := p.parseIntLiteral("LIMIT count")
	if err != nil {
		return nil, err
	}
	if count < 0 {
		return nil, fmt.Errorf("syntax error: LIMIT must be >= 0")
	}

	offset := int64(0)
	if p.current().Type == lexer.TOKEN_OFFSET {
		p.advance()
		offset, err = p.parseIntLiteral("OFFSET value")
		if err != nil {
			return nil, err
		}
		if offset < 0 {
			return nil, fmt.Errorf("syntax error: OFFSET must be >= 0")
		}
	}

	return &LimitClause{Count: int(count), Offset: int(offset)}, nil
}

func (p *sqlParser) parseIntLiteral(expected string) (int64, error) {
	tok := p.current()
	if tok.Type != lexer.TOKEN_INT_LIT {
		return 0, p.expectedError(expected, tok)
	}
	value, err := strconv.ParseInt(tok.Literal, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid integer literal '%s' at line %d, col %d", tok.Literal, tok.Line, tok.Col)
	}
	p.advance()
	return value, nil
}

func (p *sqlParser) parseInsert() (Statement, error) {
	p.advance() // INSERT
	if err := p.consume(lexer.TOKEN_INTO, "INTO"); err != nil {
		return nil, err
	}

	tableName, err := p.consumeIdent("table name")
	if err != nil {
		return nil, err
	}

	columns := make([]string, 0, 8)
	if p.current().Type == lexer.TOKEN_LPAREN {
		p.advance()
		columns, err = p.parseIdentifierListUntilRParen("column name")
		if err != nil {
			return nil, err
		}
		if err := p.consume(lexer.TOKEN_RPAREN, "')'"); err != nil {
			return nil, err
		}
	}

	if err := p.consume(lexer.TOKEN_VALUES, "VALUES"); err != nil {
		return nil, err
	}

	rows := make([][]Value, 0, 4)
	for {
		if err := p.consume(lexer.TOKEN_LPAREN, "'('"); err != nil {
			return nil, err
		}
		row, err := p.parseValueListUntilRParen()
		if err != nil {
			return nil, err
		}
		rows = append(rows, row)
		if err := p.consume(lexer.TOKEN_RPAREN, "')'"); err != nil {
			return nil, err
		}

		if p.current().Type != lexer.TOKEN_COMMA {
			break
		}
		p.advance()
	}

	if len(rows) == 0 {
		return nil, fmt.Errorf("syntax error: INSERT requires at least one VALUES row")
	}

	return &InsertStatement{TableName: tableName, Columns: columns, Rows: rows}, nil
}

func (p *sqlParser) parseUpdate() (Statement, error) {
	p.advance() // UPDATE

	tableName, err := p.consumeIdent("table name")
	if err != nil {
		return nil, err
	}

	if err := p.consume(lexer.TOKEN_SET, "SET"); err != nil {
		return nil, err
	}

	assignments := make([]Assignment, 0, 4)
	for {
		column, err := p.consumeIdent("column name")
		if err != nil {
			return nil, err
		}
		if err := p.consume(lexer.TOKEN_EQ, "'='"); err != nil {
			return nil, err
		}

		value, err := p.parseLiteralValue()
		if err != nil {
			return nil, err
		}
		assignments = append(assignments, Assignment{Column: column, Value: value})

		if p.current().Type != lexer.TOKEN_COMMA {
			break
		}
		p.advance()
	}

	var where Expression
	if p.current().Type == lexer.TOKEN_WHERE {
		p.advance()
		where, err = p.parseExpression()
		if err != nil {
			return nil, err
		}
	}

	return &UpdateStatement{TableName: tableName, Assignments: assignments, Where: where}, nil
}

func (p *sqlParser) parseDelete() (Statement, error) {
	p.advance() // DELETE
	if err := p.consume(lexer.TOKEN_FROM, "FROM"); err != nil {
		return nil, err
	}

	tableName, err := p.consumeIdent("table name")
	if err != nil {
		return nil, err
	}

	var where Expression
	if p.current().Type == lexer.TOKEN_WHERE {
		p.advance()
		where, err = p.parseExpression()
		if err != nil {
			return nil, err
		}
	}

	return &DeleteStatement{TableName: tableName, Where: where}, nil
}

func (p *sqlParser) parseExpression() (Expression, error) {
	return p.parseOr()
}

func (p *sqlParser) parseOr() (Expression, error) {
	left, err := p.parseAnd()
	if err != nil {
		return nil, err
	}

	for p.current().Type == lexer.TOKEN_OR {
		p.advance()
		right, err := p.parseAnd()
		if err != nil {
			return nil, err
		}
		left = &OrExpr{Left: left, Right: right}
	}

	return left, nil
}

func (p *sqlParser) parseAnd() (Expression, error) {
	left, err := p.parseNot()
	if err != nil {
		return nil, err
	}

	for p.current().Type == lexer.TOKEN_AND {
		p.advance()
		right, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		left = &AndExpr{Left: left, Right: right}
	}

	return left, nil
}

func (p *sqlParser) parseNot() (Expression, error) {
	if p.current().Type == lexer.TOKEN_NOT {
		p.advance()
		expr, err := p.parseNot()
		if err != nil {
			return nil, err
		}
		return &NotExpr{Expr: expr}, nil
	}
	return p.parseComparison()
}

func (p *sqlParser) parseComparison() (Expression, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}

	switch p.current().Type {
	case lexer.TOKEN_EQ, lexer.TOKEN_NEQ, lexer.TOKEN_LT, lexer.TOKEN_GT, lexer.TOKEN_LTE, lexer.TOKEN_GTE:
		op := p.current().Literal
		p.advance()
		right, err := p.parsePrimary()
		if err != nil {
			return nil, err
		}
		return &BinaryExpr{Left: left, Operator: op, Right: right}, nil
	case lexer.TOKEN_LIKE:
		p.advance()
		pattern, err := p.parseLikePattern()
		if err != nil {
			return nil, err
		}
		columnName, err := columnNameFromExpr(left)
		if err != nil {
			return nil, err
		}
		return &LikeExpr{Column: columnName, Pattern: pattern}, nil
	case lexer.TOKEN_NOT:
		if p.peek().Type != lexer.TOKEN_LIKE {
			return left, nil
		}
		p.advance() // NOT
		p.advance() // LIKE
		pattern, err := p.parseLikePattern()
		if err != nil {
			return nil, err
		}
		columnName, err := columnNameFromExpr(left)
		if err != nil {
			return nil, err
		}
		return &LikeExpr{Column: columnName, Pattern: pattern, Negated: true}, nil
	default:
		return left, nil
	}
}

func (p *sqlParser) parseLikePattern() (string, error) {
	tok := p.current()
	if tok.Type != lexer.TOKEN_STRING_LIT {
		return "", p.expectedError("LIKE pattern string", tok)
	}
	p.advance()
	return tok.Literal, nil
}

func (p *sqlParser) parsePrimary() (Expression, error) {
	tok := p.current()
	switch tok.Type {
	case lexer.TOKEN_LPAREN:
		p.advance()
		expr, err := p.parseExpression()
		if err != nil {
			return nil, err
		}
		if err := p.consume(lexer.TOKEN_RPAREN, "')'"); err != nil {
			return nil, err
		}
		return expr, nil
	case lexer.TOKEN_MATCH:
		return p.parseMatch()
	case lexer.TOKEN_IDENT:
		p.advance()
		return &ColumnRef{Name: tok.Literal}, nil
	case lexer.TOKEN_INT_LIT, lexer.TOKEN_FLOAT_LIT, lexer.TOKEN_STRING_LIT, lexer.TOKEN_TRUE, lexer.TOKEN_FALSE, lexer.TOKEN_NULL:
		value, err := tokenToValue(tok)
		if err != nil {
			return nil, err
		}
		p.advance()
		return value, nil
	default:
		return nil, p.expectedError("expression", tok)
	}
}

func (p *sqlParser) parseMatch() (Expression, error) {
	p.advance() // MATCH
	if err := p.consume(lexer.TOKEN_LPAREN, "'('"); err != nil {
		return nil, err
	}

	column, err := p.consumeIdent("MATCH column")
	if err != nil {
		return nil, err
	}
	if err := p.consume(lexer.TOKEN_COMMA, "','"); err != nil {
		return nil, err
	}

	queryTok := p.current()
	if queryTok.Type != lexer.TOKEN_STRING_LIT {
		return nil, p.expectedError("MATCH query string", queryTok)
	}
	p.advance()

	if err := p.consume(lexer.TOKEN_RPAREN, "')'"); err != nil {
		return nil, err
	}

	return &MatchExpr{
		Column: column,
		Query:  queryTok.Literal,
	}, nil
}

func (p *sqlParser) parseLiteralValue() (Value, error) {
	value, err := tokenToValue(p.current())
	if err != nil {
		return Value{}, err
	}
	p.advance()
	return value, nil
}

func (p *sqlParser) parseIdentifierList(context string) ([]string, error) {
	items := make([]string, 0, 4)
	for {
		name, err := p.consumeIdent(context)
		if err != nil {
			return nil, err
		}
		items = append(items, name)
		if p.current().Type != lexer.TOKEN_COMMA {
			break
		}
		p.advance()
	}
	return items, nil
}

func (p *sqlParser) parseIdentifierListUntilRParen(context string) ([]string, error) {
	items := make([]string, 0, 4)
	for {
		name, err := p.consumeIdent(context)
		if err != nil {
			return nil, err
		}
		items = append(items, name)

		if p.current().Type == lexer.TOKEN_COMMA {
			p.advance()
			continue
		}
		if p.current().Type == lexer.TOKEN_RPAREN {
			break
		}
		return nil, p.expectedError("',' or ')'", p.current())
	}
	return items, nil
}

func (p *sqlParser) parseValueListUntilRParen() ([]Value, error) {
	items := make([]Value, 0, 4)
	for {
		value, err := p.parseLiteralValue()
		if err != nil {
			return nil, err
		}
		items = append(items, value)

		if p.current().Type == lexer.TOKEN_COMMA {
			p.advance()
			continue
		}
		if p.current().Type == lexer.TOKEN_RPAREN {
			break
		}
		return nil, p.expectedError("',' or ')'", p.current())
	}
	return items, nil
}

func tokenToValue(tok lexer.Token) (Value, error) {
	switch tok.Type {
	case lexer.TOKEN_INT_LIT:
		n, err := strconv.ParseInt(tok.Literal, 10, 64)
		if err != nil {
			return Value{}, fmt.Errorf("invalid INT literal '%s' at line %d, col %d", tok.Literal, tok.Line, tok.Col)
		}
		return Value{Type: "int", IntVal: n}, nil
	case lexer.TOKEN_FLOAT_LIT:
		f, err := strconv.ParseFloat(tok.Literal, 64)
		if err != nil {
			return Value{}, fmt.Errorf("invalid FLOAT literal '%s' at line %d, col %d", tok.Literal, tok.Line, tok.Col)
		}
		return Value{Type: "float", FltVal: f}, nil
	case lexer.TOKEN_STRING_LIT:
		return Value{Type: "string", StrVal: tok.Literal}, nil
	case lexer.TOKEN_TRUE:
		return Value{Type: "bool", BoolVal: true}, nil
	case lexer.TOKEN_FALSE:
		return Value{Type: "bool", BoolVal: false}, nil
	case lexer.TOKEN_NULL:
		return Value{Type: "null"}, nil
	default:
		return Value{}, fmt.Errorf("syntax error at line %d, col %d: expected literal value, got '%s'", tok.Line, tok.Col, tokenDescription(tok))
	}
}

func columnNameFromExpr(expr Expression) (string, error) {
	colRef, ok := expr.(*ColumnRef)
	if !ok {
		return "", fmt.Errorf("syntax error: LIKE requires column reference on the left side")
	}
	return colRef.Name, nil
}

func (p *sqlParser) consume(tokenType lexer.TokenType, expected string) error {
	if p.current().Type != tokenType {
		return p.expectedError(expected, p.current())
	}
	p.advance()
	return nil
}

func (p *sqlParser) consumeIdent(expected string) (string, error) {
	tok := p.current()
	if tok.Type != lexer.TOKEN_IDENT {
		return "", p.expectedError(expected, tok)
	}
	p.advance()
	return tok.Literal, nil
}

func (p *sqlParser) current() lexer.Token {
	if p.pos >= len(p.tokens) {
		return lexer.Token{Type: lexer.TOKEN_EOF}
	}
	return p.tokens[p.pos]
}

func (p *sqlParser) peek() lexer.Token {
	if p.pos+1 >= len(p.tokens) {
		return lexer.Token{Type: lexer.TOKEN_EOF}
	}
	return p.tokens[p.pos+1]
}

func (p *sqlParser) advance() {
	if p.pos < len(p.tokens)-1 {
		p.pos++
	}
}

func (p *sqlParser) expectedError(expected string, got lexer.Token) error {
	if got.Type == lexer.TOKEN_EOF {
		return fmt.Errorf("syntax error: unexpected end of input, expected %s", expected)
	}
	return fmt.Errorf("syntax error at line %d, col %d: expected %s, got '%s'", got.Line, got.Col, expected, tokenDescription(got))
}

func (p *sqlParser) syntaxError(tok lexer.Token, message string) error {
	if tok.Type == lexer.TOKEN_EOF {
		return fmt.Errorf("syntax error: %s", message)
	}
	return fmt.Errorf("syntax error at line %d, col %d: %s", tok.Line, tok.Col, message)
}

func tokenDescription(tok lexer.Token) string {
	if tok.Literal != "" {
		return tok.Literal
	}
	if tok.Type == lexer.TOKEN_EOF {
		return "end of input"
	}
	return tok.Type.String()
}
