package executor

import (
	"fmt"
	"sort"
	"strings"

	"pixeldb/internal/parser"
	"pixeldb/internal/storage"
)

type queryRow struct {
	RowID       int64
	Data        storage.Row
	Score       float64
	MatchScores map[string]float64 // key: lower-cased column name
}

type CreateDatabaseCommand struct {
	stmt *parser.CreateDatabaseStatement
}

func (c *CreateDatabaseCommand) Execute(ctx *ExecutionContext) (*Result, error) {
	if err := ctx.Storage.CreateDatabase(c.stmt.DatabaseName); err != nil {
		return nil, err
	}
	return &Result{Type: "message", Message: fmt.Sprintf("Database '%s' created successfully.", c.stmt.DatabaseName)}, nil
}

type DropDatabaseCommand struct {
	stmt *parser.DropDatabaseStatement
}

func (c *DropDatabaseCommand) Execute(ctx *ExecutionContext) (*Result, error) {
	if err := ctx.Storage.DropDatabase(c.stmt.DatabaseName); err != nil {
		return nil, err
	}
	if ctx.CurrentDB != nil && strings.EqualFold(*ctx.CurrentDB, c.stmt.DatabaseName) {
		*ctx.CurrentDB = ""
	}
	return &Result{Type: "message", Message: fmt.Sprintf("Database '%s' dropped successfully.", c.stmt.DatabaseName)}, nil
}

type UseDatabaseCommand struct {
	stmt *parser.UseDatabaseStatement
}

func (c *UseDatabaseCommand) Execute(ctx *ExecutionContext) (*Result, error) {
	if !ctx.Storage.DatabaseExists(c.stmt.DatabaseName) {
		return nil, fmt.Errorf("database '%s' does not exist", c.stmt.DatabaseName)
	}
	*ctx.CurrentDB = c.stmt.DatabaseName
	return &Result{Type: "message", Message: fmt.Sprintf("Using database '%s'.", c.stmt.DatabaseName)}, nil
}

type CreateTableCommand struct {
	stmt *parser.CreateTableStatement
}

func (c *CreateTableCommand) Execute(ctx *ExecutionContext) (*Result, error) {
	dbName, err := requireCurrentDB(ctx)
	if err != nil {
		return nil, err
	}

	columns := make([]storage.ColumnSchema, 0, len(c.stmt.Columns))
	for _, column := range c.stmt.Columns {
		columns = append(columns, storage.ColumnSchema{
			Name:       column.Name,
			Type:       column.DataType,
			VarcharLen: column.VarcharLen,
		})
	}

	schema := storage.TableSchema{
		Name:     c.stmt.TableName,
		Database: dbName,
		Columns:  columns,
	}

	if err := ctx.Storage.CreateTable(dbName, schema); err != nil {
		return nil, err
	}
	return &Result{Type: "message", Message: fmt.Sprintf("Table '%s' created successfully.", c.stmt.TableName)}, nil
}

type DropTableCommand struct {
	stmt *parser.DropTableStatement
}

func (c *DropTableCommand) Execute(ctx *ExecutionContext) (*Result, error) {
	dbName, err := requireCurrentDB(ctx)
	if err != nil {
		return nil, err
	}
	if err := ctx.Storage.DropTable(dbName, c.stmt.TableName); err != nil {
		return nil, err
	}
	return &Result{Type: "message", Message: fmt.Sprintf("Table '%s' dropped successfully.", c.stmt.TableName)}, nil
}

type CreateIndexCommand struct {
	stmt *parser.CreateIndexStatement
}

func (c *CreateIndexCommand) Execute(ctx *ExecutionContext) (*Result, error) {
	dbName, err := requireCurrentDB(ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.Storage.TableExists(dbName, c.stmt.TableName) {
		return nil, fmt.Errorf("table '%s' does not exist", c.stmt.TableName)
	}
	if err := ctx.Storage.CreateIndex(dbName, c.stmt.TableName, c.stmt.IndexName, c.stmt.ColumnName); err != nil {
		return nil, err
	}
	return &Result{
		Type:    "message",
		Message: fmt.Sprintf("Index '%s' created on %s(%s).", c.stmt.IndexName, c.stmt.TableName, c.stmt.ColumnName),
	}, nil
}

type DropIndexCommand struct {
	stmt *parser.DropIndexStatement
}

func (c *DropIndexCommand) Execute(ctx *ExecutionContext) (*Result, error) {
	dbName, err := requireCurrentDB(ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.Storage.TableExists(dbName, c.stmt.TableName) {
		return nil, fmt.Errorf("table '%s' does not exist", c.stmt.TableName)
	}
	if err := ctx.Storage.DropIndex(dbName, c.stmt.TableName, c.stmt.IndexName); err != nil {
		return nil, err
	}
	return &Result{
		Type:    "message",
		Message: fmt.Sprintf("Index '%s' dropped from table '%s'.", c.stmt.IndexName, c.stmt.TableName),
	}, nil
}

type SelectCommand struct {
	stmt *parser.SelectStatement
}

func (c *SelectCommand) Execute(ctx *ExecutionContext) (*Result, error) {
	dbName, err := requireCurrentDB(ctx)
	if err != nil {
		return nil, err
	}

	if !ctx.Storage.TableExists(dbName, c.stmt.TableName) {
		return nil, fmt.Errorf("table '%s' does not exist", c.stmt.TableName)
	}

	schema, err := ctx.Storage.GetTableSchema(dbName, c.stmt.TableName)
	if err != nil {
		return nil, err
	}

	identifiedRows, err := ctx.Storage.SelectRows(dbName, c.stmt.TableName)
	if err != nil {
		return nil, err
	}

	matchScoresByRow, err := precomputeMatchScores(ctx, dbName, c.stmt.TableName, c.stmt.Where)
	if err != nil {
		return nil, err
	}

	filteredRows := make([]queryRow, 0, len(identifiedRows))
	for _, identified := range identifiedRows {
		rowScores := matchScoresByRow[identified.RowID]
		ok, err := evalExpr(c.stmt.Where, identified.Data, schema, &EvalContext{
			RowID:       identified.RowID,
			MatchScores: rowScores,
		})
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}

		filteredRows = append(filteredRows, queryRow{
			RowID:       identified.RowID,
			Data:        identified.Data,
			Score:       sumMatchScores(rowScores),
			MatchScores: rowScores,
		})
	}

	if hasAggregations(c.stmt.Items) || len(c.stmt.GroupBy) > 0 {
		return executeAggregateSelect(c.stmt, schema, filteredRows)
	}

	if err := sortQueryRows(filteredRows, c.stmt.OrderBy, schema); err != nil {
		return nil, err
	}
	filteredRows = applyLimitToQueryRows(filteredRows, c.stmt.Limit)

	columns, rowValues, err := projectRows(c.stmt.Items, schema, filteredRows)
	if err != nil {
		return nil, err
	}

	return &Result{
		Type:    "rows",
		Columns: columns,
		Rows:    rowValues,
	}, nil
}

type InsertCommand struct {
	stmt *parser.InsertStatement
}

func (c *InsertCommand) Execute(ctx *ExecutionContext) (*Result, error) {
	dbName, err := requireCurrentDB(ctx)
	if err != nil {
		return nil, err
	}

	if !ctx.Storage.TableExists(dbName, c.stmt.TableName) {
		return nil, fmt.Errorf("table '%s' does not exist", c.stmt.TableName)
	}

	schema, err := ctx.Storage.GetTableSchema(dbName, c.stmt.TableName)
	if err != nil {
		return nil, err
	}

	rowsToInsert, err := c.buildRows(schema)
	if err != nil {
		return nil, err
	}

	affected, err := ctx.Storage.InsertRows(dbName, c.stmt.TableName, rowsToInsert)
	if err != nil {
		return nil, err
	}

	return &Result{Type: "affected", Affected: affected}, nil
}

func (c *InsertCommand) buildRows(schema *storage.TableSchema) ([]storage.Row, error) {
	result := make([]storage.Row, 0, len(c.stmt.Rows))

	if len(c.stmt.Columns) == 0 {
		for rowIndex, row := range c.stmt.Rows {
			if len(row) != len(schema.Columns) {
				return nil, fmt.Errorf("insert row %d has %d values, expected %d", rowIndex, len(row), len(schema.Columns))
			}
			normalized := make(storage.Row, len(row))
			for i, value := range row {
				converted, err := parserValueToColumnType(value, schema.Columns[i])
				if err != nil {
					return nil, fmt.Errorf("column '%s': %w", schema.Columns[i].Name, err)
				}
				normalized[i] = converted
			}
			result = append(result, normalized)
		}
		return result, nil
	}

	columnIndex := make(map[string]int, len(schema.Columns))
	for idx, col := range schema.Columns {
		columnIndex[strings.ToLower(col.Name)] = idx
	}

	mappedColumns := make([]int, len(c.stmt.Columns))
	for i, name := range c.stmt.Columns {
		idx, ok := columnIndex[strings.ToLower(name)]
		if !ok {
			return nil, fmt.Errorf("unknown column '%s'", name)
		}
		mappedColumns[i] = idx
	}

	for rowIndex, row := range c.stmt.Rows {
		if len(row) != len(mappedColumns) {
			return nil, fmt.Errorf("insert row %d has %d values, expected %d", rowIndex, len(row), len(mappedColumns))
		}

		normalized := make(storage.Row, len(schema.Columns))
		for i, value := range row {
			colIdx := mappedColumns[i]
			converted, err := parserValueToColumnType(value, schema.Columns[colIdx])
			if err != nil {
				return nil, fmt.Errorf("column '%s': %w", schema.Columns[colIdx].Name, err)
			}
			normalized[colIdx] = converted
		}
		result = append(result, normalized)
	}

	return result, nil
}

type UpdateCommand struct {
	stmt *parser.UpdateStatement
}

func (c *UpdateCommand) Execute(ctx *ExecutionContext) (*Result, error) {
	dbName, err := requireCurrentDB(ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.Storage.TableExists(dbName, c.stmt.TableName) {
		return nil, fmt.Errorf("table '%s' does not exist", c.stmt.TableName)
	}

	schema, err := ctx.Storage.GetTableSchema(dbName, c.stmt.TableName)
	if err != nil {
		return nil, err
	}
	identifiedRows, err := ctx.Storage.SelectRows(dbName, c.stmt.TableName)
	if err != nil {
		return nil, err
	}

	rowIDs := make([]int64, 0, len(identifiedRows))
	for _, identified := range identifiedRows {
		match, err := evalExpr(c.stmt.Where, identified.Data, schema, nil)
		if err != nil {
			return nil, err
		}
		if match {
			rowIDs = append(rowIDs, identified.RowID)
		}
	}

	updates, err := c.buildUpdates(schema)
	if err != nil {
		return nil, err
	}

	affected, err := ctx.Storage.UpdateRows(dbName, c.stmt.TableName, rowIDs, updates)
	if err != nil {
		return nil, err
	}
	return &Result{Type: "affected", Affected: affected}, nil
}

func (c *UpdateCommand) buildUpdates(schema *storage.TableSchema) (map[string]storage.Value, error) {
	columnMap := make(map[string]storage.ColumnSchema, len(schema.Columns))
	for _, col := range schema.Columns {
		columnMap[strings.ToLower(col.Name)] = col
	}

	updates := make(map[string]storage.Value, len(c.stmt.Assignments))
	for _, assignment := range c.stmt.Assignments {
		col, ok := columnMap[strings.ToLower(assignment.Column)]
		if !ok {
			return nil, fmt.Errorf("unknown column '%s'", assignment.Column)
		}
		value, err := parserValueToColumnType(assignment.Value, col)
		if err != nil {
			return nil, fmt.Errorf("column '%s': %w", assignment.Column, err)
		}
		updates[assignment.Column] = value
	}
	return updates, nil
}

type DeleteCommand struct {
	stmt *parser.DeleteStatement
}

func (c *DeleteCommand) Execute(ctx *ExecutionContext) (*Result, error) {
	dbName, err := requireCurrentDB(ctx)
	if err != nil {
		return nil, err
	}
	if !ctx.Storage.TableExists(dbName, c.stmt.TableName) {
		return nil, fmt.Errorf("table '%s' does not exist", c.stmt.TableName)
	}

	schema, err := ctx.Storage.GetTableSchema(dbName, c.stmt.TableName)
	if err != nil {
		return nil, err
	}
	identifiedRows, err := ctx.Storage.SelectRows(dbName, c.stmt.TableName)
	if err != nil {
		return nil, err
	}

	rowIDs := make([]int64, 0, len(identifiedRows))
	for _, identified := range identifiedRows {
		match, err := evalExpr(c.stmt.Where, identified.Data, schema, nil)
		if err != nil {
			return nil, err
		}
		if match {
			rowIDs = append(rowIDs, identified.RowID)
		}
	}

	affected, err := ctx.Storage.DeleteRows(dbName, c.stmt.TableName, rowIDs)
	if err != nil {
		return nil, err
	}
	return &Result{Type: "affected", Affected: affected}, nil
}

func requireCurrentDB(ctx *ExecutionContext) (string, error) {
	if ctx.CurrentDB == nil || strings.TrimSpace(*ctx.CurrentDB) == "" {
		return "", fmt.Errorf("no active database selected; use USE <database>; first")
	}
	return *ctx.CurrentDB, nil
}

func hasAggregations(items []parser.SelectItem) bool {
	for _, item := range items {
		if item.Type == "agg" {
			return true
		}
	}
	return false
}

func precomputeMatchScores(ctx *ExecutionContext, dbName, tableName string, where parser.Expression) (map[int64]map[string]float64, error) {
	matchExprs := make([]*parser.MatchExpr, 0, 2)
	collectMatchExpressions(where, &matchExprs)
	if len(matchExprs) == 0 {
		return map[int64]map[string]float64{}, nil
	}

	byRow := make(map[int64]map[string]float64, 32)
	for _, expr := range matchExprs {
		idx, err := ctx.Storage.GetIndex(dbName, tableName, expr.Column)
		if err != nil {
			return nil, fmt.Errorf("MATCH(%s, ...): %w", expr.Column, err)
		}

		columnKey := strings.ToLower(expr.Column)
		results := idx.Search(expr.Query)
		for _, result := range results {
			scores, ok := byRow[result.RowID]
			if !ok {
				scores = make(map[string]float64)
				byRow[result.RowID] = scores
			}
			scores[columnKey] += result.Score
		}
	}
	return byRow, nil
}

func collectMatchExpressions(expr parser.Expression, out *[]*parser.MatchExpr) {
	switch e := expr.(type) {
	case nil:
		return
	case *parser.MatchExpr:
		*out = append(*out, e)
	case *parser.AndExpr:
		collectMatchExpressions(e.Left, out)
		collectMatchExpressions(e.Right, out)
	case *parser.OrExpr:
		collectMatchExpressions(e.Left, out)
		collectMatchExpressions(e.Right, out)
	case *parser.NotExpr:
		collectMatchExpressions(e.Expr, out)
	case *parser.BinaryExpr:
		collectMatchExpressions(e.Left, out)
		collectMatchExpressions(e.Right, out)
	}
}

func sumMatchScores(scores map[string]float64) float64 {
	sum := 0.0
	for _, value := range scores {
		sum += value
	}
	return sum
}

func sortQueryRows(rows []queryRow, orderBy []parser.OrderByItem, schema *storage.TableSchema) error {
	if len(orderBy) == 0 || len(rows) <= 1 {
		return nil
	}

	var sortErr error
	sort.SliceStable(rows, func(i, j int) bool {
		if sortErr != nil {
			return false
		}

		for _, item := range orderBy {
			left, err := resolveOrderValue(rows[i], item.Column, schema)
			if err != nil {
				sortErr = err
				return false
			}
			right, err := resolveOrderValue(rows[j], item.Column, schema)
			if err != nil {
				sortErr = err
				return false
			}

			cmp, err := compareOrderValues(left, right)
			if err != nil {
				sortErr = err
				return false
			}
			if cmp == 0 {
				continue
			}
			if item.Desc {
				return cmp > 0
			}
			return cmp < 0
		}
		return false
	})
	return sortErr
}

func resolveOrderValue(row queryRow, column string, schema *storage.TableSchema) (interface{}, error) {
	if strings.EqualFold(column, "_score") {
		return row.Score, nil
	}
	return resolveColumn(row.Data, schema, column)
}

func compareOrderValues(left, right interface{}) (int, error) {
	if left == nil && right == nil {
		return 0, nil
	}
	if left == nil {
		return -1, nil
	}
	if right == nil {
		return 1, nil
	}

	if lf, ok := toFloat(left); ok {
		rf, rok := toFloat(right)
		if !rok {
			return 0, fmt.Errorf("type mismatch in ORDER BY comparison: %T vs %T", left, right)
		}
		if lf < rf {
			return -1, nil
		}
		if lf > rf {
			return 1, nil
		}
		return 0, nil
	}

	switch l := left.(type) {
	case string:
		r, ok := right.(string)
		if !ok {
			return 0, fmt.Errorf("type mismatch in ORDER BY comparison: %T vs %T", left, right)
		}
		if l < r {
			return -1, nil
		}
		if l > r {
			return 1, nil
		}
		return 0, nil
	case bool:
		r, ok := right.(bool)
		if !ok {
			return 0, fmt.Errorf("type mismatch in ORDER BY comparison: %T vs %T", left, right)
		}
		if l == r {
			return 0, nil
		}
		if !l && r {
			return -1, nil
		}
		return 1, nil
	default:
		leftText := fmt.Sprintf("%v", left)
		rightText := fmt.Sprintf("%v", right)
		if leftText < rightText {
			return -1, nil
		}
		if leftText > rightText {
			return 1, nil
		}
		return 0, nil
	}
}

func applyLimitToQueryRows(rows []queryRow, limit *parser.LimitClause) []queryRow {
	if limit == nil {
		return rows
	}
	if limit.Offset >= len(rows) {
		return []queryRow{}
	}
	start := limit.Offset
	end := start + limit.Count
	if end > len(rows) {
		end = len(rows)
	}
	if end < start {
		end = start
	}
	return rows[start:end]
}

func projectRows(items []parser.SelectItem, schema *storage.TableSchema, rows []queryRow) ([]string, [][]string, error) {
	if len(items) == 1 && items[0].Type == "all" {
		return projectAllColumns(schema, rows), buildAllRows(schema, rows), nil
	}

	type projectionItem struct {
		kind   string // column | score
		colIdx int
		header string
	}
	projection := make([]projectionItem, 0, len(items))
	for _, item := range items {
		switch item.Type {
		case "column":
			colIdx, colName, err := resolveColumnIndex(schema, item.ColumnName)
			if err != nil {
				return nil, nil, err
			}
			header := colName
			if item.Alias != "" {
				header = item.Alias
			}
			projection = append(projection, projectionItem{
				kind:   "column",
				colIdx: colIdx,
				header: header,
			})
		case "score":
			header := "_score"
			if item.Alias != "" {
				header = item.Alias
			}
			projection = append(projection, projectionItem{
				kind:   "score",
				header: header,
			})
		default:
			return nil, nil, fmt.Errorf("unsupported select item type '%s' in non-aggregate query", item.Type)
		}
	}

	headers := make([]string, len(projection))
	for i, item := range projection {
		headers[i] = item.header
	}

	resultRows := make([][]string, 0, len(rows))
	for _, row := range rows {
		values := make([]string, len(projection))
		for i, item := range projection {
			switch item.kind {
			case "column":
				values[i] = valueToString(row.Data[item.colIdx])
			case "score":
				values[i] = valueToString(row.Score)
			}
		}
		resultRows = append(resultRows, values)
	}
	return headers, resultRows, nil
}

func projectAllColumns(schema *storage.TableSchema, rows []queryRow) []string {
	columns := make([]string, len(schema.Columns))
	for i, col := range schema.Columns {
		columns[i] = col.Name
	}
	return columns
}

func buildAllRows(schema *storage.TableSchema, rows []queryRow) [][]string {
	result := make([][]string, 0, len(rows))
	for _, row := range rows {
		values := make([]string, len(schema.Columns))
		for i := range schema.Columns {
			values[i] = valueToString(row.Data[i])
		}
		result = append(result, values)
	}
	return result
}

func resolveColumnIndex(schema *storage.TableSchema, name string) (int, string, error) {
	for i, col := range schema.Columns {
		if strings.EqualFold(col.Name, name) {
			return i, col.Name, nil
		}
	}
	return -1, "", fmt.Errorf("unknown column '%s'", name)
}

func parserValueToColumnType(value parser.Value, col storage.ColumnSchema) (storage.Value, error) {
	var raw storage.Value
	switch value.Type {
	case "int":
		raw = value.IntVal
	case "float":
		raw = value.FltVal
	case "string":
		raw = value.StrVal
	case "bool":
		raw = value.BoolVal
	case "null":
		raw = nil
	default:
		return nil, fmt.Errorf("unsupported value type '%s'", value.Type)
	}

	converted, err := normalizeForColumn(raw, col)
	if err != nil {
		return nil, err
	}
	return converted, nil
}

func normalizeForColumn(value storage.Value, col storage.ColumnSchema) (storage.Value, error) {
	tmpSchema := storage.TableSchema{Columns: []storage.ColumnSchema{col}}
	row := storage.Row{value}
	coerced, err := coerceRowViaEval(row, &tmpSchema)
	if err != nil {
		return nil, err
	}
	return coerced[0], nil
}

// coerceRowViaEval keeps executor independent from storage internals while sharing conversion logic.
func coerceRowViaEval(row storage.Row, schema *storage.TableSchema) (storage.Row, error) {
	coerced := make(storage.Row, len(row))
	for i, raw := range row {
		value, err := coerceToColumn(raw, schema.Columns[i])
		if err != nil {
			return nil, fmt.Errorf("column '%s': %w", schema.Columns[i].Name, err)
		}
		coerced[i] = value
	}
	return coerced, nil
}

func coerceToColumn(value storage.Value, column storage.ColumnSchema) (storage.Value, error) {
	if value == nil {
		return nil, nil
	}

	switch column.Type {
	case "INT":
		switch v := value.(type) {
		case int64:
			return v, nil
		case int:
			return int64(v), nil
		case float64:
			if float64(int64(v)) != v {
				return nil, fmt.Errorf("cannot cast FLOAT to INT without precision loss")
			}
			return int64(v), nil
		default:
			return nil, fmt.Errorf("expected INT-compatible value, got %T", value)
		}
	case "FLOAT":
		switch v := value.(type) {
		case float64:
			return v, nil
		case int64:
			return float64(v), nil
		case int:
			return float64(v), nil
		default:
			return nil, fmt.Errorf("expected FLOAT-compatible value, got %T", value)
		}
	case "BOOL":
		boolValue, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("expected BOOL value, got %T", value)
		}
		return boolValue, nil
	case "TEXT", "VARCHAR":
		stringValue, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected string value, got %T", value)
		}
		if column.Type == "VARCHAR" && column.VarcharLen > 0 && len([]rune(stringValue)) > column.VarcharLen {
			return nil, fmt.Errorf("VARCHAR(%d) overflow", column.VarcharLen)
		}
		return stringValue, nil
	default:
		return nil, fmt.Errorf("unsupported column type '%s'", column.Type)
	}
}

func valueToString(value interface{}) string {
	if value == nil {
		return "NULL"
	}
	switch v := value.(type) {
	case string:
		return v
	case bool:
		if v {
			return "true"
		}
		return "false"
	case int:
		return fmt.Sprintf("%d", v)
	case int64:
		return fmt.Sprintf("%d", v)
	case float64:
		return fmt.Sprintf("%g", v)
	default:
		return fmt.Sprintf("%v", v)
	}
}
