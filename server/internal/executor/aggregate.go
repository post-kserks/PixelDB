package executor

import (
	"fmt"
	"sort"
	"strings"

	"pixeldb/internal/parser"
	"pixeldb/internal/storage"
)

type aggregateGroup struct {
	keyValues map[string]interface{} // key: lower-cased group column name
	rows      []queryRow
}

type groupByColumn struct {
	key string
	idx int
}

func executeAggregateSelect(stmt *parser.SelectStatement, schema *storage.TableSchema, rows []queryRow) (*Result, error) {
	if len(stmt.Items) == 1 && stmt.Items[0].Type == "all" {
		return nil, fmt.Errorf("SELECT * cannot be used with GROUP BY or aggregate functions")
	}

	columnIndices := make(map[string]int, len(schema.Columns))
	columnNames := make(map[string]string, len(schema.Columns))
	for i, column := range schema.Columns {
		key := strings.ToLower(column.Name)
		columnIndices[key] = i
		columnNames[key] = column.Name
	}

	groupByCols := make([]groupByColumn, 0, len(stmt.GroupBy))
	groupBySet := make(map[string]struct{}, len(stmt.GroupBy))
	for _, col := range stmt.GroupBy {
		key := strings.ToLower(col)
		idx, ok := columnIndices[key]
		if !ok {
			return nil, fmt.Errorf("unknown GROUP BY column '%s'", col)
		}
		groupByCols = append(groupByCols, groupByColumn{key: key, idx: idx})
		groupBySet[key] = struct{}{}
	}

	hasAgg := hasAggregations(stmt.Items)
	for _, item := range stmt.Items {
		switch item.Type {
		case "column":
			key := strings.ToLower(item.ColumnName)
			if _, ok := columnIndices[key]; !ok {
				return nil, fmt.Errorf("unknown column '%s'", item.ColumnName)
			}
			if hasAgg || len(groupByCols) > 0 {
				if _, grouped := groupBySet[key]; !grouped {
					return nil, fmt.Errorf("column '%s' must appear in GROUP BY when aggregate functions are used", item.ColumnName)
				}
			}
		case "agg":
			if err := validateAggregateItem(item, columnIndices); err != nil {
				return nil, err
			}
		case "score":
			return nil, fmt.Errorf("_score cannot be selected together with GROUP BY or aggregate functions")
		default:
			return nil, fmt.Errorf("unsupported select item type '%s' in aggregate query", item.Type)
		}
	}

	groupOrder, groups, err := buildAggregateGroups(rows, groupByCols)
	if err != nil {
		return nil, err
	}
	if len(groupByCols) == 0 && len(groupOrder) == 0 {
		groupOrder = []string{"__all__"}
		groups["__all__"] = &aggregateGroup{keyValues: map[string]interface{}{}, rows: rows}
	}

	headers := make([]string, len(stmt.Items))
	orderLookup := make(map[string]int, len(stmt.Items)*3)
	for i, item := range stmt.Items {
		header := aggregateHeader(item, columnNames)
		headers[i] = header

		orderLookup[strings.ToLower(header)] = i
		if item.Alias != "" {
			orderLookup[strings.ToLower(item.Alias)] = i
		}
		if item.Type == "column" {
			orderLookup[strings.ToLower(item.ColumnName)] = i
		}
		if item.Type == "agg" {
			aggName := strings.ToUpper(item.AggFunc)
			orderLookup[strings.ToLower(fmt.Sprintf("%s(%s)", aggName, item.ColumnName))] = i
		}
	}

	aggregatedRows := make([][]interface{}, 0, len(groupOrder))
	for _, groupKey := range groupOrder {
		group := groups[groupKey]
		values := make([]interface{}, len(stmt.Items))
		for i, item := range stmt.Items {
			switch item.Type {
			case "column":
				values[i] = group.keyValues[strings.ToLower(item.ColumnName)]
			case "agg":
				value, err := evaluateAggregate(item, group.rows, schema, columnIndices)
				if err != nil {
					return nil, err
				}
				values[i] = value
			}
		}
		aggregatedRows = append(aggregatedRows, values)
	}

	if err := sortAggregateRows(aggregatedRows, stmt.OrderBy, orderLookup); err != nil {
		return nil, err
	}
	aggregatedRows = applyLimitToInterfaceRows(aggregatedRows, stmt.Limit)

	resultRows := make([][]string, 0, len(aggregatedRows))
	for _, row := range aggregatedRows {
		formatted := make([]string, len(row))
		for i, value := range row {
			formatted[i] = valueToString(value)
		}
		resultRows = append(resultRows, formatted)
	}

	return &Result{
		Type:    "rows",
		Columns: headers,
		Rows:    resultRows,
	}, nil
}

func validateAggregateItem(item parser.SelectItem, columnIndices map[string]int) error {
	agg := strings.ToUpper(item.AggFunc)
	switch agg {
	case "COUNT":
		if item.ColumnName == "*" {
			return nil
		}
		if _, ok := columnIndices[strings.ToLower(item.ColumnName)]; !ok {
			return fmt.Errorf("unknown aggregate column '%s'", item.ColumnName)
		}
		return nil
	case "SUM", "AVG", "MIN", "MAX":
		if item.ColumnName == "*" {
			return fmt.Errorf("%s(*) is not supported", agg)
		}
		if _, ok := columnIndices[strings.ToLower(item.ColumnName)]; !ok {
			return fmt.Errorf("unknown aggregate column '%s'", item.ColumnName)
		}
		return nil
	default:
		return fmt.Errorf("unsupported aggregate function '%s'", item.AggFunc)
	}
}

func buildAggregateGroups(rows []queryRow, groupByCols []groupByColumn) ([]string, map[string]*aggregateGroup, error) {
	if len(groupByCols) == 0 {
		return nil, map[string]*aggregateGroup{}, nil
	}

	order := make([]string, 0, len(rows))
	groups := make(map[string]*aggregateGroup, len(rows))
	for _, row := range rows {
		keyBuilder := strings.Builder{}
		keyValues := make(map[string]interface{}, len(groupByCols))
		for _, groupByCol := range groupByCols {
			if groupByCol.idx < 0 || groupByCol.idx >= len(row.Data) {
				return nil, nil, fmt.Errorf("group by column index out of bounds")
			}

			value := row.Data[groupByCol.idx]
			keyValues[groupByCol.key] = value
			keyBuilder.WriteString(fmt.Sprintf("%T:%v|", value, value))
		}

		key := keyBuilder.String()
		group, exists := groups[key]
		if !exists {
			group = &aggregateGroup{keyValues: keyValues}
			groups[key] = group
			order = append(order, key)
		}
		group.rows = append(group.rows, row)
	}
	return order, groups, nil
}

func aggregateHeader(item parser.SelectItem, columnNames map[string]string) string {
	if item.Alias != "" {
		return item.Alias
	}
	switch item.Type {
	case "column":
		if canonical, ok := columnNames[strings.ToLower(item.ColumnName)]; ok {
			return canonical
		}
		return item.ColumnName
	case "agg":
		return fmt.Sprintf("%s(%s)", strings.ToUpper(item.AggFunc), item.ColumnName)
	default:
		return item.ColumnName
	}
}

func evaluateAggregate(item parser.SelectItem, rows []queryRow, schema *storage.TableSchema, columnIndices map[string]int) (interface{}, error) {
	agg := strings.ToUpper(item.AggFunc)
	columnName := strings.ToLower(item.ColumnName)

	switch agg {
	case "COUNT":
		if item.ColumnName == "*" {
			return int64(len(rows)), nil
		}
		colIdx := columnIndices[columnName]
		count := int64(0)
		for _, row := range rows {
			if row.Data[colIdx] != nil {
				count++
			}
		}
		return count, nil
	case "SUM":
		colIdx := columnIndices[columnName]
		if schema.Columns[colIdx].Type != "INT" && schema.Columns[colIdx].Type != "FLOAT" {
			return nil, fmt.Errorf("SUM(%s) requires numeric column", item.ColumnName)
		}
		sum := 0.0
		hasFloat := false
		hasValues := false
		for _, row := range rows {
			value := row.Data[colIdx]
			if value == nil {
				continue
			}
			number, isFloat, ok := numericValue(value)
			if !ok {
				return nil, fmt.Errorf("SUM(%s) requires numeric values", item.ColumnName)
			}
			sum += number
			hasFloat = hasFloat || isFloat
			hasValues = true
		}
		if !hasValues {
			return nil, nil
		}
		if !hasFloat {
			return int64(sum), nil
		}
		return sum, nil
	case "AVG":
		colIdx := columnIndices[columnName]
		if schema.Columns[colIdx].Type != "INT" && schema.Columns[colIdx].Type != "FLOAT" {
			return nil, fmt.Errorf("AVG(%s) requires numeric column", item.ColumnName)
		}
		sum := 0.0
		count := 0
		for _, row := range rows {
			value := row.Data[colIdx]
			if value == nil {
				continue
			}
			number, _, ok := numericValue(value)
			if !ok {
				return nil, fmt.Errorf("AVG(%s) requires numeric values", item.ColumnName)
			}
			sum += number
			count++
		}
		if count == 0 {
			return nil, nil
		}
		return sum / float64(count), nil
	case "MIN", "MAX":
		colIdx := columnIndices[columnName]
		var current interface{}
		hasValues := false
		for _, row := range rows {
			value := row.Data[colIdx]
			if value == nil {
				continue
			}
			if !hasValues {
				current = value
				hasValues = true
				continue
			}
			cmp, err := compareOrderValues(value, current)
			if err != nil {
				return nil, fmt.Errorf("%s(%s) comparison error: %w", agg, item.ColumnName, err)
			}
			if agg == "MIN" && cmp < 0 {
				current = value
			}
			if agg == "MAX" && cmp > 0 {
				current = value
			}
		}
		if !hasValues {
			return nil, nil
		}
		return current, nil
	default:
		return nil, fmt.Errorf("unsupported aggregate function '%s'", item.AggFunc)
	}
}

func numericValue(value interface{}) (float64, bool, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), false, true
	case int64:
		return float64(v), false, true
	case float64:
		return v, true, true
	default:
		return 0, false, false
	}
}

func sortAggregateRows(rows [][]interface{}, orderBy []parser.OrderByItem, lookup map[string]int) error {
	if len(orderBy) == 0 || len(rows) <= 1 {
		return nil
	}

	var sortErr error
	sort.SliceStable(rows, func(i, j int) bool {
		if sortErr != nil {
			return false
		}
		for _, item := range orderBy {
			idx, ok := lookup[strings.ToLower(item.Column)]
			if !ok {
				sortErr = fmt.Errorf("unknown ORDER BY column '%s'", item.Column)
				return false
			}
			cmp, err := compareOrderValues(rows[i][idx], rows[j][idx])
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

func applyLimitToInterfaceRows(rows [][]interface{}, limit *parser.LimitClause) [][]interface{} {
	if limit == nil {
		return rows
	}
	if limit.Offset >= len(rows) {
		return [][]interface{}{}
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
