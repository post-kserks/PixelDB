package storage

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"pixeldb/internal/analyzer"
	"pixeldb/internal/index"
)

type FileStorageEngine struct {
	rootDir string

	tableLocks   map[string]*sync.RWMutex
	tableLocksMu sync.Mutex

	globalMu sync.RWMutex
}

type databaseMeta struct {
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type tableDataDisk struct {
	Rows   [][]interface{} `json:"rows"`
	RowIDs []int64         `json:"row_ids,omitempty"`
	NextID int64           `json:"next_id"`
}

type tableIndexesDisk struct {
	Indexes map[string]string `json:"indexes"` // index name -> column name
}

func NewFileStorageEngine(rootDir string) *FileStorageEngine {
	s := &FileStorageEngine{
		rootDir:    rootDir,
		tableLocks: make(map[string]*sync.RWMutex),
	}
	_ = os.MkdirAll(s.databasesDir(), 0o755)
	return s
}

func (s *FileStorageEngine) CreateDatabase(name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("database name cannot be empty")
	}

	s.globalMu.Lock()
	defer s.globalMu.Unlock()

	path := s.dbDir(name)
	if dirExists(path) {
		return fmt.Errorf("database '%s' already exists", name)
	}

	if err := os.MkdirAll(path, 0o755); err != nil {
		return fmt.Errorf("create database directory: %w", err)
	}

	meta := databaseMeta{Name: name, CreatedAt: time.Now().UTC()}
	if err := writeJSONAtomic(filepath.Join(path, "_meta.json"), meta); err != nil {
		return fmt.Errorf("write database metadata: %w", err)
	}

	return nil
}

func (s *FileStorageEngine) DropDatabase(name string) error {
	s.globalMu.Lock()
	defer s.globalMu.Unlock()

	path := s.dbDir(name)
	if !dirExists(path) {
		return fmt.Errorf("database '%s' does not exist", name)
	}

	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("drop database '%s': %w", name, err)
	}

	s.tableLocksMu.Lock()
	for key := range s.tableLocks {
		if strings.HasPrefix(key, name+"/") {
			delete(s.tableLocks, key)
		}
	}
	s.tableLocksMu.Unlock()

	return nil
}

func (s *FileStorageEngine) DatabaseExists(name string) bool {
	s.globalMu.RLock()
	defer s.globalMu.RUnlock()
	return dirExists(s.dbDir(name))
}

func (s *FileStorageEngine) ListDatabases() ([]string, error) {
	s.globalMu.RLock()
	defer s.globalMu.RUnlock()

	entries, err := os.ReadDir(s.databasesDir())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []string{}, nil
		}
		return nil, fmt.Errorf("read databases directory: %w", err)
	}

	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

func (s *FileStorageEngine) CreateTable(dbName string, schema TableSchema) error {
	s.globalMu.Lock()
	defer s.globalMu.Unlock()

	if !dirExists(s.dbDir(dbName)) {
		return fmt.Errorf("database '%s' does not exist", dbName)
	}
	if strings.TrimSpace(schema.Name) == "" {
		return fmt.Errorf("table name cannot be empty")
	}
	if len(schema.Columns) == 0 {
		return fmt.Errorf("table '%s' must have at least one column", schema.Name)
	}

	tablePath := s.tableDir(dbName, schema.Name)
	if dirExists(tablePath) {
		return fmt.Errorf("table '%s' already exists", schema.Name)
	}

	if err := os.MkdirAll(tablePath, 0o755); err != nil {
		return fmt.Errorf("create table directory: %w", err)
	}

	schema.Database = dbName
	if schema.CreatedAt.IsZero() {
		schema.CreatedAt = time.Now().UTC()
	}

	if err := writeJSONAtomic(s.schemaPath(dbName, schema.Name), schema); err != nil {
		return fmt.Errorf("write schema: %w", err)
	}
	if err := writeJSONAtomic(s.dataPath(dbName, schema.Name), tableDataDisk{Rows: [][]interface{}{}, RowIDs: []int64{}, NextID: 1}); err != nil {
		return fmt.Errorf("write table data: %w", err)
	}
	if err := writeJSONAtomic(s.indexesPath(dbName, schema.Name), tableIndexesDisk{Indexes: map[string]string{}}); err != nil {
		return fmt.Errorf("write table indexes metadata: %w", err)
	}

	s.getTableLock(dbName, schema.Name)
	return nil
}

func (s *FileStorageEngine) DropTable(dbName, tableName string) error {
	s.globalMu.Lock()
	defer s.globalMu.Unlock()

	if !dirExists(s.dbDir(dbName)) {
		return fmt.Errorf("database '%s' does not exist", dbName)
	}
	path := s.tableDir(dbName, tableName)
	if !dirExists(path) {
		return fmt.Errorf("table '%s' does not exist", tableName)
	}

	if err := os.RemoveAll(path); err != nil {
		return fmt.Errorf("drop table '%s': %w", tableName, err)
	}

	s.tableLocksMu.Lock()
	delete(s.tableLocks, tableLockKey(dbName, tableName))
	s.tableLocksMu.Unlock()
	return nil
}

func (s *FileStorageEngine) TableExists(dbName, tableName string) bool {
	s.globalMu.RLock()
	defer s.globalMu.RUnlock()
	return dirExists(s.tableDir(dbName, tableName))
}

func (s *FileStorageEngine) GetTableSchema(dbName, tableName string) (*TableSchema, error) {
	s.globalMu.RLock()
	defer s.globalMu.RUnlock()
	return s.readSchema(dbName, tableName)
}

func (s *FileStorageEngine) InsertRows(dbName, tableName string, rows []Row) (int, error) {
	if len(rows) == 0 {
		return 0, nil
	}

	lock := s.getTableLock(dbName, tableName)
	lock.Lock()
	defer lock.Unlock()

	schema, err := s.readSchema(dbName, tableName)
	if err != nil {
		return 0, err
	}
	data, err := s.readData(dbName, tableName, schema)
	if err != nil {
		return 0, err
	}

	for _, row := range rows {
		if len(row) != len(schema.Columns) {
			return 0, fmt.Errorf("invalid row width for table '%s': expected %d values, got %d", tableName, len(schema.Columns), len(row))
		}
		normalized := make([]interface{}, len(row))
		for i, value := range row {
			n, err := normalizeValue(value, schema.Columns[i])
			if err != nil {
				return 0, fmt.Errorf("column '%s': %w", schema.Columns[i].Name, err)
			}
			normalized[i] = n
		}
		data.Rows = append(data.Rows, normalized)
		data.RowIDs = append(data.RowIDs, data.NextID)
		data.NextID++
	}

	if err := writeJSONAtomic(s.dataPath(dbName, tableName), data); err != nil {
		return 0, fmt.Errorf("write table data: %w", err)
	}
	if err := s.syncIndexesLocked(dbName, tableName, schema, data); err != nil {
		return 0, err
	}

	return len(rows), nil
}

func (s *FileStorageEngine) SelectRows(dbName, tableName string) ([]IdentifiedRow, error) {
	lock := s.getTableLock(dbName, tableName)
	lock.RLock()
	defer lock.RUnlock()

	schema, err := s.readSchema(dbName, tableName)
	if err != nil {
		return nil, err
	}
	data, err := s.readData(dbName, tableName, schema)
	if err != nil {
		return nil, err
	}

	rows := make([]IdentifiedRow, 0, len(data.Rows))
	for i, rawRow := range data.Rows {
		coerced, err := coerceRow(rawRow, schema)
		if err != nil {
			return nil, err
		}
		copied := make(Row, len(coerced))
		copy(copied, coerced)
		rows = append(rows, IdentifiedRow{RowID: data.RowIDs[i], Data: copied})
	}

	return rows, nil
}

func (s *FileStorageEngine) UpdateRows(dbName, tableName string, rowIDs []int64, updates map[string]Value) (int, error) {
	if len(rowIDs) == 0 {
		return 0, nil
	}

	lock := s.getTableLock(dbName, tableName)
	lock.Lock()
	defer lock.Unlock()

	schema, err := s.readSchema(dbName, tableName)
	if err != nil {
		return 0, err
	}
	data, err := s.readData(dbName, tableName, schema)
	if err != nil {
		return 0, err
	}

	columnIndex := make(map[string]int, len(schema.Columns))
	for i, col := range schema.Columns {
		columnIndex[strings.ToLower(col.Name)] = i
	}

	normalizedUpdates := make(map[int]interface{}, len(updates))
	for colName, value := range updates {
		idx, ok := columnIndex[strings.ToLower(colName)]
		if !ok {
			return 0, fmt.Errorf("unknown column '%s'", colName)
		}
		normalized, err := normalizeValue(value, schema.Columns[idx])
		if err != nil {
			return 0, fmt.Errorf("column '%s': %w", schema.Columns[idx].Name, err)
		}
		normalizedUpdates[idx] = normalized
	}

	targetIDs := make(map[int64]struct{}, len(rowIDs))
	for _, id := range rowIDs {
		targetIDs[id] = struct{}{}
	}

	updated := 0
	for i, id := range data.RowIDs {
		if _, ok := targetIDs[id]; !ok {
			continue
		}
		for colIdx, value := range normalizedUpdates {
			data.Rows[i][colIdx] = value
		}
		updated++
	}

	if err := writeJSONAtomic(s.dataPath(dbName, tableName), data); err != nil {
		return 0, fmt.Errorf("write table data: %w", err)
	}
	if err := s.syncIndexesLocked(dbName, tableName, schema, data); err != nil {
		return 0, err
	}

	return updated, nil
}

func (s *FileStorageEngine) DeleteRows(dbName, tableName string, rowIDs []int64) (int, error) {
	if len(rowIDs) == 0 {
		return 0, nil
	}

	lock := s.getTableLock(dbName, tableName)
	lock.Lock()
	defer lock.Unlock()

	schema, err := s.readSchema(dbName, tableName)
	if err != nil {
		return 0, err
	}
	data, err := s.readData(dbName, tableName, schema)
	if err != nil {
		return 0, err
	}

	targetIDs := make(map[int64]struct{}, len(rowIDs))
	for _, id := range rowIDs {
		targetIDs[id] = struct{}{}
	}

	filteredRows := make([][]interface{}, 0, len(data.Rows))
	filteredIDs := make([]int64, 0, len(data.RowIDs))
	deleted := 0
	for i, id := range data.RowIDs {
		if _, remove := targetIDs[id]; remove {
			deleted++
			continue
		}
		filteredRows = append(filteredRows, data.Rows[i])
		filteredIDs = append(filteredIDs, id)
	}
	data.Rows = filteredRows
	data.RowIDs = filteredIDs

	if err := writeJSONAtomic(s.dataPath(dbName, tableName), data); err != nil {
		return 0, fmt.Errorf("write table data: %w", err)
	}
	if err := s.syncIndexesLocked(dbName, tableName, schema, data); err != nil {
		return 0, err
	}

	return deleted, nil
}

func (s *FileStorageEngine) CreateIndex(dbName, tableName, indexName, columnName string) error {
	if strings.TrimSpace(indexName) == "" {
		return fmt.Errorf("index name cannot be empty")
	}
	if strings.TrimSpace(columnName) == "" {
		return fmt.Errorf("column name cannot be empty")
	}

	lock := s.getTableLock(dbName, tableName)
	lock.Lock()
	defer lock.Unlock()

	schema, err := s.readSchema(dbName, tableName)
	if err != nil {
		return err
	}
	columnIdx, column, err := resolveSchemaColumn(schema, columnName)
	if err != nil {
		return err
	}
	if column.Type != "TEXT" && column.Type != "VARCHAR" {
		return fmt.Errorf("column '%s' must be TEXT or VARCHAR for full-text index", column.Name)
	}

	indexesMeta, err := s.readIndexesMeta(dbName, tableName)
	if err != nil {
		return err
	}

	for existingName, existingColumn := range indexesMeta.Indexes {
		if strings.EqualFold(existingName, indexName) {
			return fmt.Errorf("index '%s' already exists", existingName)
		}
		if strings.EqualFold(existingColumn, column.Name) {
			return fmt.Errorf("column '%s' is already indexed by '%s'", column.Name, existingName)
		}
	}

	data, err := s.readData(dbName, tableName, schema)
	if err != nil {
		return err
	}

	idx := index.NewInvertedIndex(column.Name, analyzer.NewStandardAnalyzer())
	for i, row := range data.Rows {
		rowID := data.RowIDs[i]
		text := textFromIndexedValue(row[columnIdx])
		idx.AddDocument(rowID, text)
	}
	if err := s.saveIndexLocked(dbName, tableName, column.Name, idx); err != nil {
		return err
	}

	if indexesMeta.Indexes == nil {
		indexesMeta.Indexes = make(map[string]string)
	}
	indexesMeta.Indexes[indexName] = column.Name
	if err := s.writeIndexesMeta(dbName, tableName, indexesMeta); err != nil {
		return err
	}

	return nil
}

func (s *FileStorageEngine) DropIndex(dbName, tableName, indexName string) error {
	if strings.TrimSpace(indexName) == "" {
		return fmt.Errorf("index name cannot be empty")
	}

	lock := s.getTableLock(dbName, tableName)
	lock.Lock()
	defer lock.Unlock()

	indexesMeta, err := s.readIndexesMeta(dbName, tableName)
	if err != nil {
		return err
	}

	foundName := ""
	columnName := ""
	for existingName, existingColumn := range indexesMeta.Indexes {
		if strings.EqualFold(existingName, indexName) {
			foundName = existingName
			columnName = existingColumn
			break
		}
	}
	if foundName == "" {
		return fmt.Errorf("index '%s' does not exist", indexName)
	}

	if err := os.Remove(s.indexPath(dbName, tableName, columnName)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove index file: %w", err)
	}

	delete(indexesMeta.Indexes, foundName)
	return s.writeIndexesMeta(dbName, tableName, indexesMeta)
}

func (s *FileStorageEngine) GetIndex(dbName, tableName, columnName string) (*index.InvertedIndex, error) {
	lock := s.getTableLock(dbName, tableName)
	lock.RLock()
	defer lock.RUnlock()

	indexesMeta, err := s.readIndexesMeta(dbName, tableName)
	if err != nil {
		return nil, err
	}

	actualColumn := ""
	for _, col := range indexesMeta.Indexes {
		if strings.EqualFold(col, columnName) {
			actualColumn = col
			break
		}
	}
	if actualColumn == "" {
		return nil, fmt.Errorf("index for column '%s' does not exist", columnName)
	}

	idx, err := index.Load(s.indexPath(dbName, tableName, actualColumn), analyzer.NewStandardAnalyzer())
	if err != nil {
		return nil, fmt.Errorf("load index for column '%s': %w", actualColumn, err)
	}
	return idx, nil
}

func (s *FileStorageEngine) SaveIndex(dbName, tableName, columnName string, idx *index.InvertedIndex) error {
	lock := s.getTableLock(dbName, tableName)
	lock.Lock()
	defer lock.Unlock()

	return s.saveIndexLocked(dbName, tableName, columnName, idx)
}

func (s *FileStorageEngine) ListIndexes(dbName, tableName string) ([]string, error) {
	lock := s.getTableLock(dbName, tableName)
	lock.RLock()
	defer lock.RUnlock()

	meta, err := s.readIndexesMeta(dbName, tableName)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(meta.Indexes))
	for name := range meta.Indexes {
		names = append(names, name)
	}
	sort.Strings(names)
	return names, nil
}

func (s *FileStorageEngine) databasesDir() string {
	return filepath.Join(s.rootDir, "databases")
}

func (s *FileStorageEngine) dbDir(dbName string) string {
	return filepath.Join(s.databasesDir(), dbName)
}

func (s *FileStorageEngine) tableDir(dbName, tableName string) string {
	return filepath.Join(s.dbDir(dbName), tableName)
}

func (s *FileStorageEngine) schemaPath(dbName, tableName string) string {
	return filepath.Join(s.tableDir(dbName, tableName), "_schema.json")
}

func (s *FileStorageEngine) dataPath(dbName, tableName string) string {
	return filepath.Join(s.tableDir(dbName, tableName), "_data.json")
}

func (s *FileStorageEngine) indexesPath(dbName, tableName string) string {
	return filepath.Join(s.tableDir(dbName, tableName), "_indexes.json")
}

func (s *FileStorageEngine) indexPath(dbName, tableName, columnName string) string {
	return filepath.Join(s.tableDir(dbName, tableName), fmt.Sprintf("_index_%s.json", strings.ToLower(columnName)))
}

func (s *FileStorageEngine) getTableLock(dbName, tableName string) *sync.RWMutex {
	key := tableLockKey(dbName, tableName)
	s.tableLocksMu.Lock()
	defer s.tableLocksMu.Unlock()
	if lock, ok := s.tableLocks[key]; ok {
		return lock
	}
	lock := &sync.RWMutex{}
	s.tableLocks[key] = lock
	return lock
}

func (s *FileStorageEngine) readSchema(dbName, tableName string) (*TableSchema, error) {
	path := s.schemaPath(dbName, tableName)
	bytes, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("table '%s' does not exist", tableName)
		}
		return nil, fmt.Errorf("read schema for table '%s': %w", tableName, err)
	}

	var schema TableSchema
	if err := json.Unmarshal(bytes, &schema); err != nil {
		return nil, fmt.Errorf("decode schema for table '%s': %w", tableName, err)
	}
	return &schema, nil
}

func (s *FileStorageEngine) readData(dbName, tableName string, schema *TableSchema) (*tableDataDisk, error) {
	path := s.dataPath(dbName, tableName)
	bytes, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("table '%s' data does not exist", tableName)
		}
		return nil, fmt.Errorf("read data for table '%s': %w", tableName, err)
	}

	var data tableDataDisk
	if err := json.Unmarshal(bytes, &data); err != nil {
		return nil, fmt.Errorf("decode data for table '%s': %w", tableName, err)
	}

	// Migration: synthesize row_ids for legacy data files that lack them.
	if len(data.RowIDs) != len(data.Rows) {
		data.RowIDs = make([]int64, len(data.Rows))
		for i := range data.Rows {
			data.RowIDs[i] = int64(i + 1)
		}
		if data.NextID < int64(len(data.Rows)+1) {
			data.NextID = int64(len(data.Rows) + 1)
		}
	}
	if data.NextID <= 0 {
		data.NextID = 1
	}

	for i, row := range data.Rows {
		coerced, err := coerceRow(row, schema)
		if err != nil {
			return nil, fmt.Errorf("coerce row %d in table '%s': %w", i, tableName, err)
		}
		copyRow := make([]interface{}, len(coerced))
		for j := range coerced {
			copyRow[j] = coerced[j]
		}
		data.Rows[i] = copyRow
	}

	return &data, nil
}

func (s *FileStorageEngine) readIndexesMeta(dbName, tableName string) (*tableIndexesDisk, error) {
	path := s.indexesPath(dbName, tableName)
	bytes, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return &tableIndexesDisk{Indexes: map[string]string{}}, nil
		}
		return nil, fmt.Errorf("read indexes metadata for table '%s': %w", tableName, err)
	}

	var meta tableIndexesDisk
	if err := json.Unmarshal(bytes, &meta); err != nil {
		return nil, fmt.Errorf("decode indexes metadata for table '%s': %w", tableName, err)
	}
	if meta.Indexes == nil {
		meta.Indexes = make(map[string]string)
	}
	return &meta, nil
}

func (s *FileStorageEngine) writeIndexesMeta(dbName, tableName string, meta *tableIndexesDisk) error {
	if meta == nil {
		meta = &tableIndexesDisk{Indexes: map[string]string{}}
	}
	if meta.Indexes == nil {
		meta.Indexes = make(map[string]string)
	}
	if err := writeJSONAtomic(s.indexesPath(dbName, tableName), meta); err != nil {
		return fmt.Errorf("write indexes metadata: %w", err)
	}
	return nil
}

func (s *FileStorageEngine) saveIndexLocked(dbName, tableName, columnName string, idx *index.InvertedIndex) error {
	if idx == nil {
		return fmt.Errorf("cannot save nil index for column '%s'", columnName)
	}
	if err := index.Save(s.indexPath(dbName, tableName, columnName), idx); err != nil {
		return fmt.Errorf("save index for column '%s': %w", columnName, err)
	}
	return nil
}

func (s *FileStorageEngine) syncIndexesLocked(dbName, tableName string, schema *TableSchema, data *tableDataDisk) error {
	meta, err := s.readIndexesMeta(dbName, tableName)
	if err != nil {
		return err
	}
	if len(meta.Indexes) == 0 {
		return nil
	}

	for _, columnName := range meta.Indexes {
		columnIdx, column, err := resolveSchemaColumn(schema, columnName)
		if err != nil {
			return err
		}

		idx := index.NewInvertedIndex(column.Name, analyzer.NewStandardAnalyzer())
		for rowPos, row := range data.Rows {
			text := textFromIndexedValue(row[columnIdx])
			idx.AddDocument(data.RowIDs[rowPos], text)
		}
		if err := s.saveIndexLocked(dbName, tableName, column.Name, idx); err != nil {
			return err
		}
	}

	return nil
}

func resolveSchemaColumn(schema *TableSchema, columnName string) (int, ColumnSchema, error) {
	for i, col := range schema.Columns {
		if strings.EqualFold(col.Name, columnName) {
			return i, col, nil
		}
	}
	return -1, ColumnSchema{}, fmt.Errorf("unknown column '%s'", columnName)
}

func textFromIndexedValue(value interface{}) string {
	if value == nil {
		return ""
	}
	text, ok := value.(string)
	if !ok {
		return fmt.Sprintf("%v", value)
	}
	return text
}

func coerceRow(raw []interface{}, schema *TableSchema) (Row, error) {
	if len(raw) != len(schema.Columns) {
		return nil, fmt.Errorf("row width mismatch: expected %d, got %d", len(schema.Columns), len(raw))
	}
	row := make(Row, len(raw))
	for i, cell := range raw {
		v, err := coerceValue(cell, schema.Columns[i])
		if err != nil {
			return nil, fmt.Errorf("column '%s': %w", schema.Columns[i].Name, err)
		}
		row[i] = v
	}
	return row, nil
}

func coerceValue(raw interface{}, col ColumnSchema) (Value, error) {
	return normalizeValue(raw, col)
}

func normalizeValue(value interface{}, col ColumnSchema) (Value, error) {
	if value == nil {
		return nil, nil
	}

	switch col.Type {
	case "INT":
		intVal, ok := toInt64(value)
		if !ok {
			return nil, fmt.Errorf("expected INT, got %T", value)
		}
		return intVal, nil
	case "FLOAT":
		floatVal, ok := toFloat64(value)
		if !ok {
			return nil, fmt.Errorf("expected FLOAT, got %T", value)
		}
		return floatVal, nil
	case "BOOL":
		boolVal, ok := value.(bool)
		if !ok {
			return nil, fmt.Errorf("expected BOOL, got %T", value)
		}
		return boolVal, nil
	case "TEXT", "VARCHAR":
		strVal, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("expected %s, got %T", col.Type, value)
		}
		if col.Type == "VARCHAR" && col.VarcharLen > 0 {
			if len([]rune(strVal)) > col.VarcharLen {
				return nil, fmt.Errorf("VARCHAR(%d) overflow", col.VarcharLen)
			}
		}
		return strVal, nil
	default:
		return nil, fmt.Errorf("unsupported column type '%s'", col.Type)
	}
}

func toInt64(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int:
		return int64(v), true
	case int8:
		return int64(v), true
	case int16:
		return int64(v), true
	case int32:
		return int64(v), true
	case int64:
		return v, true
	case uint:
		if v > math.MaxInt64 {
			return 0, false
		}
		return int64(v), true
	case uint8:
		return int64(v), true
	case uint16:
		return int64(v), true
	case uint32:
		return int64(v), true
	case uint64:
		if v > math.MaxInt64 {
			return 0, false
		}
		return int64(v), true
	case float32:
		f := float64(v)
		if math.Trunc(f) != f {
			return 0, false
		}
		return int64(f), true
	case float64:
		if math.Trunc(v) != v {
			return 0, false
		}
		return int64(v), true
	default:
		return 0, false
	}
}

func toFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case int:
		return float64(v), true
	case int8:
		return float64(v), true
	case int16:
		return float64(v), true
	case int32:
		return float64(v), true
	case int64:
		return float64(v), true
	case uint:
		return float64(v), true
	case uint8:
		return float64(v), true
	case uint16:
		return float64(v), true
	case uint32:
		return float64(v), true
	case uint64:
		return float64(v), true
	case float32:
		return float64(v), true
	case float64:
		return v, true
	default:
		return 0, false
	}
}

func writeJSONAtomic(path string, payload interface{}) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}

	bytes, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, bytes, 0o644); err != nil {
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return nil
}

func dirExists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.IsDir()
}

func tableLockKey(dbName, tableName string) string {
	return dbName + "/" + tableName
}
