package parser

// Statement is the root interface for all SQL statements.
type Statement interface {
	statementNode()
	StatementType() string
}

// DDL.
type CreateDatabaseStatement struct {
	DatabaseName string
}

type DropDatabaseStatement struct {
	DatabaseName string
}

type UseDatabaseStatement struct {
	DatabaseName string
}

type ColumnDef struct {
	Name       string
	DataType   string // INT, FLOAT, BOOL, TEXT, VARCHAR
	VarcharLen int
}

type CreateTableStatement struct {
	TableName string
	Columns   []ColumnDef
}

type DropTableStatement struct {
	TableName string
}

type CreateIndexStatement struct {
	IndexName  string
	TableName  string
	ColumnName string
}

type DropIndexStatement struct {
	IndexName string
	TableName string
}

// DML.
type SelectItem struct {
	Type       string // all | column | agg | score
	ColumnName string
	AggFunc    string
	Alias      string
}

type OrderByItem struct {
	Column string
	Desc   bool
}

type LimitClause struct {
	Count  int
	Offset int
}

type SelectStatement struct {
	Items     []SelectItem
	TableName string
	Where     Expression
	GroupBy   []string
	OrderBy   []OrderByItem
	Limit     *LimitClause
}

type InsertStatement struct {
	TableName string
	Columns   []string // empty means all columns in schema order
	Rows      [][]Value
}

type Assignment struct {
	Column string
	Value  Value
}

type UpdateStatement struct {
	TableName   string
	Assignments []Assignment
	Where       Expression
}

type DeleteStatement struct {
	TableName string
	Where     Expression
}

func (*CreateDatabaseStatement) statementNode() {}
func (*DropDatabaseStatement) statementNode()   {}
func (*UseDatabaseStatement) statementNode()    {}
func (*CreateTableStatement) statementNode()    {}
func (*DropTableStatement) statementNode()      {}
func (*CreateIndexStatement) statementNode()    {}
func (*DropIndexStatement) statementNode()      {}
func (*SelectStatement) statementNode()         {}
func (*InsertStatement) statementNode()         {}
func (*UpdateStatement) statementNode()         {}
func (*DeleteStatement) statementNode()         {}

func (*CreateDatabaseStatement) StatementType() string { return "CREATE_DATABASE" }
func (*DropDatabaseStatement) StatementType() string   { return "DROP_DATABASE" }
func (*UseDatabaseStatement) StatementType() string    { return "USE_DATABASE" }
func (*CreateTableStatement) StatementType() string    { return "CREATE_TABLE" }
func (*DropTableStatement) StatementType() string      { return "DROP_TABLE" }
func (*CreateIndexStatement) StatementType() string    { return "CREATE_INDEX" }
func (*DropIndexStatement) StatementType() string      { return "DROP_INDEX" }
func (*SelectStatement) StatementType() string         { return "SELECT" }
func (*InsertStatement) StatementType() string         { return "INSERT" }
func (*UpdateStatement) StatementType() string         { return "UPDATE" }
func (*DeleteStatement) StatementType() string         { return "DELETE" }

// Expression is the root interface for all WHERE expressions.
type Expression interface {
	expressionNode()
}

// Value is both a literal expression and a transport type used in INSERT/UPDATE AST nodes.
type Value struct {
	Type    string // int, float, string, bool, null
	IntVal  int64
	FltVal  float64
	StrVal  string
	BoolVal bool
}

// ColumnRef references a table column.
type ColumnRef struct {
	Name string
}

// BinaryExpr represents comparison operators: =, !=, <, >, <=, >=.
type BinaryExpr struct {
	Left     Expression
	Operator string
	Right    Expression
}

type MatchExpr struct {
	Column string
	Query  string
}

type LikeExpr struct {
	Column  string
	Pattern string
	Negated bool
}

type AndExpr struct {
	Left  Expression
	Right Expression
}

type OrExpr struct {
	Left  Expression
	Right Expression
}

type NotExpr struct {
	Expr Expression
}

func (Value) expressionNode()       {}
func (*ColumnRef) expressionNode()  {}
func (*BinaryExpr) expressionNode() {}
func (*MatchExpr) expressionNode()  {}
func (*LikeExpr) expressionNode()   {}
func (*AndExpr) expressionNode()    {}
func (*OrExpr) expressionNode()     {}
func (*NotExpr) expressionNode()    {}
