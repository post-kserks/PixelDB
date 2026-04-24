package storage

import (
	"time"

	"pixeldb/internal/index"
)

// Value is a single cell value in a row.
// Supported runtime types: int64, float64, string, bool, nil.
type Value interface{}

// Row is a single row in table order.
type Row []Value

// IdentifiedRow is a row with a stable, auto-increment ID that survives deletions.
type IdentifiedRow struct {
	RowID int64
	Data  Row
}

// ColumnSchema describes one table column.
type ColumnSchema struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	VarcharLen int    `json:"length,omitempty"`
}

// TableSchema describes a table.
type TableSchema struct {
	Name      string         `json:"name"`
	Database  string         `json:"database"`
	Columns   []ColumnSchema `json:"columns"`
	CreatedAt time.Time      `json:"created_at"`
}

// StorageEngine is the abstraction used by executor.
type StorageEngine interface {
	CreateDatabase(name string) error
	DropDatabase(name string) error
	DatabaseExists(name string) bool
	ListDatabases() ([]string, error)

	CreateTable(dbName string, schema TableSchema) error
	DropTable(dbName, tableName string) error
	TableExists(dbName, tableName string) bool
	GetTableSchema(dbName, tableName string) (*TableSchema, error)

	InsertRows(dbName, tableName string, rows []Row) (int, error)
	SelectRows(dbName, tableName string) ([]IdentifiedRow, error)
	UpdateRows(dbName, tableName string, rowIDs []int64, updates map[string]Value) (int, error)
	DeleteRows(dbName, tableName string, rowIDs []int64) (int, error)

	CreateIndex(dbName, tableName, indexName, columnName string) error
	DropIndex(dbName, tableName, indexName string) error
	GetIndex(dbName, tableName, columnName string) (*index.InvertedIndex, error)
	SaveIndex(dbName, tableName, columnName string, idx *index.InvertedIndex) error
	ListIndexes(dbName, tableName string) ([]string, error)
}
