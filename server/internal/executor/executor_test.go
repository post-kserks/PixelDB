package executor

import (
	"testing"

	"pixeldb/internal/parser"
	"pixeldb/internal/storage"
)

func executeSQL(t *testing.T, session *Session, sql string) *Result {
	t.Helper()
	stmt, err := parser.Parse(sql)
	if err != nil {
		t.Fatalf("Parse failed for %q: %v", sql, err)
	}
	result, err := session.Execute(stmt)
	if err != nil {
		t.Fatalf("Execute failed for %q: %v", sql, err)
	}
	return result
}

func executeSQLError(t *testing.T, session *Session, sql string) error {
	t.Helper()
	stmt, err := parser.Parse(sql)
	if err != nil {
		return err
	}
	_, err = session.Execute(stmt)
	return err
}

func setupSession(t *testing.T) *Session {
	t.Helper()
	store := storage.NewFileStorageEngine(t.TempDir())
	session := NewSession(store)

	executeSQL(t, session, "CREATE DATABASE mydb;")
	executeSQL(t, session, "USE mydb;")
	executeSQL(t, session, "CREATE TABLE heroes (id INT, name VARCHAR(100), level INT, alive BOOL, score FLOAT, bio TEXT, year INT);")
	return session
}

func seedHeroes(t *testing.T, session *Session) {
	t.Helper()
	executeSQL(t, session, "INSERT INTO heroes VALUES (1, 'Aragorn', 10, TRUE, 9.8, 'Warrior king of Gondor', 2022);")
	executeSQL(t, session, "INSERT INTO heroes VALUES (2, 'Legolas', 9, TRUE, 9.5, 'Elven archer of Mirkwood', 2021);")
	executeSQL(t, session, "INSERT INTO heroes VALUES (3, 'Gimli', 8, TRUE, 8.2, 'Dwarf warrior and dragon slayer', 2023);")
	executeSQL(t, session, "INSERT INTO heroes VALUES (4, 'Boromir', 5, FALSE, 7.1, 'Captain of Gondor', 2019);")
}

func TestBasicCRUDAndSelect(t *testing.T) {
	session := setupSession(t)
	seedHeroes(t, session)

	result := executeSQL(t, session, "SELECT * FROM heroes;")
	if result.Type != "rows" {
		t.Fatalf("expected rows result, got %s", result.Type)
	}
	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 rows, got %d", len(result.Rows))
	}

	update := executeSQL(t, session, "UPDATE heroes SET level = 11 WHERE id = 1;")
	if update.Affected != 1 {
		t.Fatalf("expected 1 affected row, got %d", update.Affected)
	}

	selected := executeSQL(t, session, "SELECT level FROM heroes WHERE id = 1;")
	if len(selected.Rows) != 1 || selected.Rows[0][0] != "11" {
		t.Fatalf("expected level=11, got %#v", selected.Rows)
	}

	deleted := executeSQL(t, session, "DELETE FROM heroes WHERE alive = FALSE;")
	if deleted.Affected != 1 {
		t.Fatalf("expected 1 deleted row, got %d", deleted.Affected)
	}
}

func TestLikeOrderByLimit(t *testing.T) {
	session := setupSession(t)
	seedHeroes(t, session)

	result := executeSQL(t, session, "SELECT name FROM heroes WHERE name LIKE '%or%' ORDER BY level DESC LIMIT 1;")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Aragorn" {
		t.Fatalf("expected Aragorn, got %s", result.Rows[0][0])
	}
}

func TestLimitOffset(t *testing.T) {
	session := setupSession(t)
	seedHeroes(t, session)

	result := executeSQL(t, session, "SELECT name FROM heroes ORDER BY id ASC LIMIT 2 OFFSET 1;")
	if len(result.Rows) != 2 {
		t.Fatalf("expected 2 rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "Legolas" || result.Rows[1][0] != "Gimli" {
		t.Fatalf("unexpected rows: %#v", result.Rows)
	}
}

func TestMatchRequiresIndex(t *testing.T) {
	session := setupSession(t)
	seedHeroes(t, session)

	if err := executeSQLError(t, session, "SELECT * FROM heroes WHERE MATCH(bio, 'gondor');"); err == nil {
		t.Fatal("expected MATCH error without index")
	}
}

func TestMatchSearchAndScoreOrdering(t *testing.T) {
	session := setupSession(t)
	seedHeroes(t, session)

	executeSQL(t, session, "CREATE INDEX idx_bio ON heroes(bio);")
	result := executeSQL(t, session, "SELECT name, _score FROM heroes WHERE MATCH(bio, 'warrior gondor') ORDER BY _score DESC;")
	if len(result.Rows) == 0 {
		t.Fatal("expected non-empty search result")
	}
	if result.Rows[0][0] != "Aragorn" {
		t.Fatalf("expected Aragorn as best match, got %s", result.Rows[0][0])
	}
}

func TestAggregationsWithoutGroupBy(t *testing.T) {
	session := setupSession(t)
	seedHeroes(t, session)

	result := executeSQL(t, session, "SELECT COUNT(*) AS cnt FROM heroes WHERE alive = TRUE;")
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
		t.Fatalf("unexpected result shape: %#v", result.Rows)
	}
	if result.Rows[0][0] != "3" {
		t.Fatalf("expected count=3, got %s", result.Rows[0][0])
	}
}

func TestAggregationsWithGroupBy(t *testing.T) {
	session := setupSession(t)
	seedHeroes(t, session)

	result := executeSQL(t, session, "SELECT level, COUNT(*) AS cnt FROM heroes GROUP BY level ORDER BY level ASC;")
	if len(result.Rows) != 4 {
		t.Fatalf("expected 4 grouped rows, got %d", len(result.Rows))
	}
	if result.Rows[0][0] != "5" || result.Rows[0][1] != "1" {
		t.Fatalf("unexpected first grouped row: %#v", result.Rows[0])
	}
}

func TestScoreWithoutMatchReturnsZero(t *testing.T) {
	session := setupSession(t)
	seedHeroes(t, session)

	result := executeSQL(t, session, "SELECT name, _score FROM heroes ORDER BY id ASC LIMIT 1;")
	if len(result.Rows) != 1 {
		t.Fatalf("expected 1 row, got %d", len(result.Rows))
	}
	if result.Rows[0][1] != "0" {
		t.Fatalf("expected _score=0 without MATCH, got %s", result.Rows[0][1])
	}
}

func TestIndexLifecycleViaExecutor(t *testing.T) {
	session := setupSession(t)
	seedHeroes(t, session)

	executeSQL(t, session, "CREATE INDEX idx_bio ON heroes(bio);")
	matchResult := executeSQL(t, session, "SELECT name FROM heroes WHERE MATCH(bio, 'dragon');")
	if len(matchResult.Rows) != 1 || matchResult.Rows[0][0] != "Gimli" {
		t.Fatalf("unexpected MATCH results: %#v", matchResult.Rows)
	}

	executeSQL(t, session, "DROP INDEX idx_bio ON heroes;")
	if err := executeSQLError(t, session, "SELECT name FROM heroes WHERE MATCH(bio, 'dragon');"); err == nil {
		t.Fatal("expected MATCH error after dropping index")
	}
}
