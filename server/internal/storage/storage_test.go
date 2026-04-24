package storage

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func testSchema(dbName string) TableSchema {
	return TableSchema{
		Name:     "heroes",
		Database: dbName,
		Columns: []ColumnSchema{
			{Name: "id", Type: "INT"},
			{Name: "name", Type: "VARCHAR", VarcharLen: 100},
			{Name: "level", Type: "INT"},
			{Name: "alive", Type: "BOOL"},
		},
	}
}

func TestDatabaseLifecycle(t *testing.T) {
	store := NewFileStorageEngine(t.TempDir())

	if store.DatabaseExists("mydb") {
		t.Fatal("database should not exist")
	}
	if err := store.CreateDatabase("mydb"); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}
	if !store.DatabaseExists("mydb") {
		t.Fatal("database should exist")
	}

	dbs, err := store.ListDatabases()
	if err != nil {
		t.Fatalf("ListDatabases failed: %v", err)
	}
	if len(dbs) != 1 || dbs[0] != "mydb" {
		t.Fatalf("unexpected db list: %#v", dbs)
	}

	if err := store.DropDatabase("mydb"); err != nil {
		t.Fatalf("DropDatabase failed: %v", err)
	}
	if store.DatabaseExists("mydb") {
		t.Fatal("database should be removed")
	}
}

func TestTableLifecycleAndDataOperations(t *testing.T) {
	store := NewFileStorageEngine(t.TempDir())
	if err := store.CreateDatabase("mydb"); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	schema := testSchema("mydb")
	if err := store.CreateTable("mydb", schema); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	if !store.TableExists("mydb", "heroes") {
		t.Fatal("table should exist")
	}

	inserted, err := store.InsertRows("mydb", "heroes", []Row{
		{int64(1), "Aragorn", int64(10), true},
		{int64(2), "Legolas", int64(9), true},
	})
	if err != nil {
		t.Fatalf("InsertRows failed: %v", err)
	}
	if inserted != 2 {
		t.Fatalf("expected 2 inserted rows, got %d", inserted)
	}

	rows, err := store.SelectRows("mydb", "heroes")
	if err != nil {
		t.Fatalf("SelectRows failed: %v", err)
	}
	if len(rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(rows))
	}
	if rows[0].RowID != 1 || rows[1].RowID != 2 {
		t.Fatalf("unexpected row IDs: %d, %d", rows[0].RowID, rows[1].RowID)
	}

	// Update by rowID (row with RowID=2 is Legolas)
	updated, err := store.UpdateRows("mydb", "heroes", []int64{2}, map[string]Value{"level": int64(11)})
	if err != nil {
		t.Fatalf("UpdateRows failed: %v", err)
	}
	if updated != 1 {
		t.Fatalf("expected 1 updated row, got %d", updated)
	}

	rows, err = store.SelectRows("mydb", "heroes")
	if err != nil {
		t.Fatalf("SelectRows failed: %v", err)
	}
	if rows[1].Data[2].(int64) != 11 {
		t.Fatalf("expected updated level=11, got %#v", rows[1].Data[2])
	}

	// Delete by rowID (row with RowID=1 is Aragorn)
	deleted, err := store.DeleteRows("mydb", "heroes", []int64{1})
	if err != nil {
		t.Fatalf("DeleteRows failed: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 deleted row, got %d", deleted)
	}

	rows, err = store.SelectRows("mydb", "heroes")
	if err != nil {
		t.Fatalf("SelectRows failed: %v", err)
	}
	if len(rows) != 1 || rows[0].Data[1].(string) != "Legolas" {
		t.Fatalf("unexpected rows after delete: %#v", rows)
	}
	// Surviving row keeps its original RowID
	if rows[0].RowID != 2 {
		t.Fatalf("expected surviving row to have RowID=2, got %d", rows[0].RowID)
	}

	if err := store.DropTable("mydb", "heroes"); err != nil {
		t.Fatalf("DropTable failed: %v", err)
	}
	if store.TableExists("mydb", "heroes") {
		t.Fatal("table should be removed")
	}
}

func TestPersistenceAcrossInstances(t *testing.T) {
	root := t.TempDir()

	store1 := NewFileStorageEngine(root)
	if err := store1.CreateDatabase("mydb"); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}
	if err := store1.CreateTable("mydb", testSchema("mydb")); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}
	if _, err := store1.InsertRows("mydb", "heroes", []Row{{int64(1), "Aragorn", int64(10), true}}); err != nil {
		t.Fatalf("InsertRows failed: %v", err)
	}

	store2 := NewFileStorageEngine(root)
	rows, err := store2.SelectRows("mydb", "heroes")
	if err != nil {
		t.Fatalf("SelectRows failed: %v", err)
	}
	if len(rows) != 1 || rows[0].Data[1].(string) != "Aragorn" {
		t.Fatalf("unexpected persisted rows: %#v", rows)
	}
	if rows[0].RowID != 1 {
		t.Fatalf("expected RowID=1, got %d", rows[0].RowID)
	}
}

func TestParallelInsertRows(t *testing.T) {
	store := NewFileStorageEngine(t.TempDir())
	if err := store.CreateDatabase("mydb"); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}
	if err := store.CreateTable("mydb", testSchema("mydb")); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		i := i
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := store.InsertRows("mydb", "heroes", []Row{{int64(i + 1), "Hero", int64(i), true}})
			if err != nil {
				t.Errorf("InsertRows failed: %v", err)
			}
		}()
	}
	wg.Wait()

	rows, err := store.SelectRows("mydb", "heroes")
	if err != nil {
		t.Fatalf("SelectRows failed: %v", err)
	}
	if len(rows) != 20 {
		t.Fatalf("expected 20 rows, got %d", len(rows))
	}

	// Verify all RowIDs are unique
	seen := make(map[int64]bool, len(rows))
	for _, r := range rows {
		if seen[r.RowID] {
			t.Fatalf("duplicate RowID: %d", r.RowID)
		}
		seen[r.RowID] = true
	}
}

func TestRowIDStabilityAfterDelete(t *testing.T) {
	store := NewFileStorageEngine(t.TempDir())
	if err := store.CreateDatabase("mydb"); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}
	if err := store.CreateTable("mydb", testSchema("mydb")); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	// Insert 3 rows: RowIDs 1, 2, 3
	_, err := store.InsertRows("mydb", "heroes", []Row{
		{int64(1), "Aragorn", int64(10), true},
		{int64(2), "Legolas", int64(9), true},
		{int64(3), "Gimli", int64(8), true},
	})
	if err != nil {
		t.Fatalf("InsertRows failed: %v", err)
	}

	// Delete middle row (RowID=2)
	deleted, err := store.DeleteRows("mydb", "heroes", []int64{2})
	if err != nil {
		t.Fatalf("DeleteRows failed: %v", err)
	}
	if deleted != 1 {
		t.Fatalf("expected 1 deleted, got %d", deleted)
	}

	// Insert new row — should get RowID=4, not 2
	_, err = store.InsertRows("mydb", "heroes", []Row{
		{int64(4), "Boromir", int64(5), false},
	})
	if err != nil {
		t.Fatalf("InsertRows failed: %v", err)
	}

	rows, err := store.SelectRows("mydb", "heroes")
	if err != nil {
		t.Fatalf("SelectRows failed: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}

	// Verify: Aragorn=1, Gimli=3, Boromir=4
	expectedIDs := []int64{1, 3, 4}
	expectedNames := []string{"Aragorn", "Gimli", "Boromir"}
	for i, r := range rows {
		if r.RowID != expectedIDs[i] {
			t.Fatalf("row %d: expected RowID=%d, got %d", i, expectedIDs[i], r.RowID)
		}
		if r.Data[1].(string) != expectedNames[i] {
			t.Fatalf("row %d: expected name=%s, got %s", i, expectedNames[i], r.Data[1].(string))
		}
	}
}

func TestIndexLifecycleAndAutoUpdate(t *testing.T) {
	store := NewFileStorageEngine(t.TempDir())
	if err := store.CreateDatabase("mydb"); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}

	schema := TableSchema{
		Name:     "articles",
		Database: "mydb",
		Columns: []ColumnSchema{
			{Name: "id", Type: "INT"},
			{Name: "title", Type: "TEXT"},
			{Name: "body", Type: "TEXT"},
		},
	}
	if err := store.CreateTable("mydb", schema); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	_, err := store.InsertRows("mydb", "articles", []Row{
		{int64(1), "One", "dragon rider"},
		{int64(2), "Two", "elven archer"},
	})
	if err != nil {
		t.Fatalf("InsertRows failed: %v", err)
	}

	if err := store.CreateIndex("mydb", "articles", "idx_body", "body"); err != nil {
		t.Fatalf("CreateIndex failed: %v", err)
	}

	indexes, err := store.ListIndexes("mydb", "articles")
	if err != nil {
		t.Fatalf("ListIndexes failed: %v", err)
	}
	if len(indexes) != 1 || indexes[0] != "idx_body" {
		t.Fatalf("unexpected indexes: %#v", indexes)
	}

	idx, err := store.GetIndex("mydb", "articles", "body")
	if err != nil {
		t.Fatalf("GetIndex failed: %v", err)
	}
	results := idx.Search("dragon")
	if len(results) != 1 || results[0].RowID != 1 {
		t.Fatalf("unexpected search results: %#v", results)
	}

	// insert should auto-sync index
	_, err = store.InsertRows("mydb", "articles", []Row{
		{int64(3), "Three", "dragon tamer"},
	})
	if err != nil {
		t.Fatalf("InsertRows failed: %v", err)
	}
	idx, err = store.GetIndex("mydb", "articles", "body")
	if err != nil {
		t.Fatalf("GetIndex failed: %v", err)
	}
	if got := len(idx.Search("dragon")); got != 2 {
		t.Fatalf("expected 2 docs after insert, got %d", got)
	}

	// update should auto-sync index
	rows, err := store.SelectRows("mydb", "articles")
	if err != nil {
		t.Fatalf("SelectRows failed: %v", err)
	}
	var rowID3 int64
	for _, row := range rows {
		if row.Data[0].(int64) == 3 {
			rowID3 = row.RowID
			break
		}
	}
	if rowID3 == 0 {
		t.Fatal("failed to find row id for article 3")
	}

	_, err = store.UpdateRows("mydb", "articles", []int64{rowID3}, map[string]Value{"body": "forest wanderer"})
	if err != nil {
		t.Fatalf("UpdateRows failed: %v", err)
	}
	idx, err = store.GetIndex("mydb", "articles", "body")
	if err != nil {
		t.Fatalf("GetIndex failed: %v", err)
	}
	if got := len(idx.Search("dragon")); got != 1 {
		t.Fatalf("expected 1 doc after update, got %d", got)
	}

	// delete should auto-sync index
	var rowID1 int64
	for _, row := range rows {
		if row.Data[0].(int64) == 1 {
			rowID1 = row.RowID
			break
		}
	}
	if rowID1 == 0 {
		t.Fatal("failed to find row id for article 1")
	}
	if _, err := store.DeleteRows("mydb", "articles", []int64{rowID1}); err != nil {
		t.Fatalf("DeleteRows failed: %v", err)
	}
	idx, err = store.GetIndex("mydb", "articles", "body")
	if err != nil {
		t.Fatalf("GetIndex failed: %v", err)
	}
	if got := len(idx.Search("dragon")); got != 0 {
		t.Fatalf("expected 0 docs after delete, got %d", got)
	}

	if err := store.DropIndex("mydb", "articles", "idx_body"); err != nil {
		t.Fatalf("DropIndex failed: %v", err)
	}
	if _, err := store.GetIndex("mydb", "articles", "body"); err == nil {
		t.Fatal("expected error after dropping index")
	}
}

func TestLegacyDataMigrationWithoutRowIDs(t *testing.T) {
	root := t.TempDir()
	store := NewFileStorageEngine(root)
	if err := store.CreateDatabase("mydb"); err != nil {
		t.Fatalf("CreateDatabase failed: %v", err)
	}
	if err := store.CreateTable("mydb", testSchema("mydb")); err != nil {
		t.Fatalf("CreateTable failed: %v", err)
	}

	legacyData := `{"rows":[[1,"Aragorn",10,true],[2,"Legolas",9,true]],"next_id":0}`
	dataPath := filepath.Join(root, "databases", "mydb", "heroes", "_data.json")
	if err := os.WriteFile(dataPath, []byte(legacyData), 0o644); err != nil {
		t.Fatalf("failed to write legacy data: %v", err)
	}

	_, err := store.InsertRows("mydb", "heroes", []Row{{int64(3), "Gimli", int64(8), true}})
	if err != nil {
		t.Fatalf("InsertRows failed on migrated data: %v", err)
	}

	rows, err := store.SelectRows("mydb", "heroes")
	if err != nil {
		t.Fatalf("SelectRows failed: %v", err)
	}
	if len(rows) != 3 {
		t.Fatalf("expected 3 rows, got %d", len(rows))
	}
	if rows[0].RowID != 1 || rows[1].RowID != 2 || rows[2].RowID != 3 {
		t.Fatalf("unexpected migrated row IDs: %d, %d, %d", rows[0].RowID, rows[1].RowID, rows[2].RowID)
	}
}
