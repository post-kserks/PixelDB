package parser

import "testing"

func TestParseValidStatements(t *testing.T) {
	queries := []string{
		"CREATE DATABASE mydb;",
		"DROP DATABASE mydb;",
		"USE mydb;",
		"CREATE TABLE heroes (id INT, name VARCHAR(100), alive BOOL);",
		"DROP TABLE heroes;",
		"CREATE INDEX idx_bio ON heroes(bio);",
		"DROP INDEX idx_bio ON heroes;",
		"SELECT * FROM heroes;",
		"SELECT id, name FROM heroes WHERE level > 5;",
		"SELECT * FROM heroes WHERE alive = TRUE AND level >= 3;",
		"SELECT * FROM heroes WHERE NOT (level < 2) OR name = 'Gimli';",
		"SELECT * FROM heroes WHERE MATCH(bio, 'warrior king');",
		"SELECT * FROM heroes WHERE name LIKE '%ara%';",
		"SELECT COUNT(*) FROM heroes;",
		"SELECT level, COUNT(*) AS cnt FROM heroes GROUP BY level ORDER BY cnt DESC LIMIT 10 OFFSET 5;",
		"INSERT INTO heroes VALUES (1, 'Aragorn', 10);",
		"INSERT INTO heroes (id, name) VALUES (1, 'test'), (2, 'test2');",
		"UPDATE heroes SET level = 11 WHERE id = 1;",
		"DELETE FROM heroes WHERE alive = FALSE;",
	}

	for _, query := range queries {
		query := query
		t.Run(query, func(t *testing.T) {
			if _, err := Parse(query); err != nil {
				t.Fatalf("Parse(%q) returned error: %v", query, err)
			}
		})
	}
}

func TestParseSelectShape(t *testing.T) {
	stmt, err := Parse("SELECT id, COUNT(*) AS cnt FROM heroes WHERE level > 5 GROUP BY id ORDER BY cnt DESC LIMIT 3 OFFSET 1;")
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	sel, ok := stmt.(*SelectStatement)
	if !ok {
		t.Fatalf("expected *SelectStatement, got %T", stmt)
	}
	if len(sel.Items) != 2 {
		t.Fatalf("unexpected select items: %#v", sel.Items)
	}
	if sel.Items[0].Type != "column" || sel.Items[0].ColumnName != "id" {
		t.Fatalf("unexpected first item: %#v", sel.Items[0])
	}
	if sel.Items[1].Type != "agg" || sel.Items[1].AggFunc != "COUNT" || sel.Items[1].Alias != "cnt" {
		t.Fatalf("unexpected second item: %#v", sel.Items[1])
	}
	if sel.TableName != "heroes" {
		t.Fatalf("unexpected table name: %s", sel.TableName)
	}
	if sel.Where == nil {
		t.Fatal("expected WHERE expression")
	}
	if len(sel.GroupBy) != 1 || sel.GroupBy[0] != "id" {
		t.Fatalf("unexpected group by: %#v", sel.GroupBy)
	}
	if len(sel.OrderBy) != 1 || sel.OrderBy[0].Column != "cnt" || !sel.OrderBy[0].Desc {
		t.Fatalf("unexpected order by: %#v", sel.OrderBy)
	}
	if sel.Limit == nil || sel.Limit.Count != 3 || sel.Limit.Offset != 1 {
		t.Fatalf("unexpected limit clause: %#v", sel.Limit)
	}
}

func TestParseMatchAndLikeExpressions(t *testing.T) {
	stmt, err := Parse("SELECT * FROM heroes WHERE MATCH(bio, 'dragon') AND name NOT LIKE '%orc%';")
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	sel := stmt.(*SelectStatement)
	andExpr, ok := sel.Where.(*AndExpr)
	if !ok {
		t.Fatalf("expected AndExpr, got %T", sel.Where)
	}

	if _, ok := andExpr.Left.(*MatchExpr); !ok {
		t.Fatalf("expected MatchExpr on left, got %T", andExpr.Left)
	}
	like, ok := andExpr.Right.(*LikeExpr)
	if !ok {
		t.Fatalf("expected LikeExpr on right, got %T", andExpr.Right)
	}
	if !like.Negated {
		t.Fatal("expected NOT LIKE to set Negated=true")
	}
}

func TestParseErrors(t *testing.T) {
	cases := []string{
		"",
		"SELECT * FROM heroes",
		"CREATE TABLE heroes (id DOUBLE);",
		"INSERT INTO heroes VALUES ();",
		"SELECT * FROM heroes OFFSET 5;",
		"CREATE INDEX idx ON heroes;",
	}

	for _, query := range cases {
		if _, err := Parse(query); err == nil {
			t.Fatalf("expected parsing error for %q", query)
		}
	}
}
