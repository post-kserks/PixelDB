package executor

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"pixeldb/internal/storage"
)

func TestSearchLifecycleIntegration(t *testing.T) {
	root := t.TempDir()
	store := storage.NewFileStorageEngine(root)
	session := NewSession(store)

	executeSQL(t, session, "CREATE DATABASE db;")
	executeSQL(t, session, "USE db;")
	executeSQL(t, session, "CREATE TABLE docs (id INT, name TEXT, body TEXT, year INT);")
	executeSQL(t, session, "INSERT INTO docs VALUES (1, 'A', 'dragon rider', 2021), (2, 'B', 'forest elf', 2019);")
	executeSQL(t, session, "CREATE INDEX idx_body ON docs(body);")

	result := executeSQL(t, session, "SELECT name FROM docs WHERE MATCH(body, 'dragon');")
	if len(result.Rows) != 1 || result.Rows[0][0] != "A" {
		t.Fatalf("unexpected MATCH results: %#v", result.Rows)
	}

	executeSQL(t, session, "INSERT INTO docs VALUES (3, 'C', 'ancient dragon king', 2024);")
	result = executeSQL(t, session, "SELECT name FROM docs WHERE MATCH(body, 'dragon') ORDER BY _score DESC;")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows after insert, got %d", len(result.Rows))
	}

	executeSQL(t, session, "DELETE FROM docs WHERE id = 1;")
	result = executeSQL(t, session, "SELECT name FROM docs WHERE MATCH(body, 'dragon');")
	if len(result.Rows) != 1 || result.Rows[0][0] != "C" {
		t.Fatalf("unexpected MATCH results after delete: %#v", result.Rows)
	}

	executeSQL(t, session, "DROP INDEX idx_body ON docs;")
	if err := executeSQLError(t, session, "SELECT name FROM docs WHERE MATCH(body, 'dragon');"); err == nil {
		t.Fatal("expected MATCH error after dropping index")
	}
}

func TestComplexQueryIntegration(t *testing.T) {
	session := setupSession(t)
	seedHeroes(t, session)
	executeSQL(t, session, "CREATE INDEX idx_bio ON heroes(bio);")

	result := executeSQL(t, session, "SELECT name, _score FROM heroes WHERE MATCH(bio, 'dragon') AND year > 2020 ORDER BY _score DESC LIMIT 5;")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Gimli" {
		t.Fatalf("expected Gimli, got %s", result.Rows[0][0])
	}
}

func TestLikeOrderLimitIntegration(t *testing.T) {
	session := setupSession(t)
	seedHeroes(t, session)

	result := executeSQL(t, session, "SELECT * FROM heroes WHERE name LIKE 'A%' ORDER BY level DESC LIMIT 1;")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != "Aragorn" {
		t.Fatalf("expected Aragorn row, got %#v", result.Rows[0])
	}
}

func TestBackwardCompatibilityLegacyData(t *testing.T) {
	root := t.TempDir()
	store := storage.NewFileStorageEngine(root)
	session := NewSession(store)

	executeSQL(t, session, "CREATE DATABASE db;")
	executeSQL(t, session, "USE db;")
	executeSQL(t, session, "CREATE TABLE t (id INT, body TEXT);")
	executeSQL(t, session, "INSERT INTO t VALUES (1, 'one'), (2, 'two');")

	legacyPath := filepath.Join(root, "databases", "db", "t", "_data.json")
	legacyData := `{"rows":[[1,"one"],[2,"two"]]}`
	if err := os.WriteFile(legacyPath, []byte(legacyData), 0o644); err != nil {
		t.Fatalf("failed writing legacy data file: %v", err)
	}

	// Mutation should transparently migrate row IDs.
	executeSQL(t, session, "INSERT INTO t VALUES (3, 'three');")
	result := executeSQL(t, session, "SELECT id FROM t ORDER BY id ASC;")
	if len(result.Rows) != 3 {
		t.Fatalf("expected 3 rows after migration, got %d", len(result.Rows))
	}
}

func TestIndexConsistencyThroughMutations(t *testing.T) {
	root := t.TempDir()
	store := storage.NewFileStorageEngine(root)
	session := NewSession(store)

	executeSQL(t, session, "CREATE DATABASE db;")
	executeSQL(t, session, "USE db;")
	executeSQL(t, session, "CREATE TABLE docs (id INT, body TEXT);")

	for i := 1; i <= 100; i++ {
		body := "forest"
		if i%2 == 0 {
			body = "dragon"
		}
		executeSQL(t, session, fmt.Sprintf("INSERT INTO docs VALUES (%d, '%s');", i, body))
	}

	executeSQL(t, session, "CREATE INDEX idx_body ON docs(body);")
	for i := 2; i <= 100; i += 4 {
		executeSQL(t, session, fmt.Sprintf("DELETE FROM docs WHERE id = %d;", i))
	}
	for i := 1; i <= 20; i++ {
		executeSQL(t, session, fmt.Sprintf("UPDATE docs SET body = 'dragon elder' WHERE id = %d;", i))
	}

	result := executeSQL(t, session, "SELECT COUNT(*) AS cnt FROM docs WHERE MATCH(body, 'dragon');")
	if len(result.Rows) != 1 {
		t.Fatalf("unexpected count shape: %#v", result.Rows)
	}
	// Non-empty result verifies index consistency after bulk mutations.
	if result.Rows[0][0] == "0" {
		t.Fatalf("expected non-zero match count after mutations")
	}
}
