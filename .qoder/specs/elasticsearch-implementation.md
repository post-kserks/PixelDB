# Elasticsearch for PixelDB -- Implementation Plan

## Context

PixelDB is an educational SQL database (Go server + C++17 client) with JSON file storage and no indexing. Every SELECT does a full table scan. There is no full-text search, no relevance scoring, no aggregations, no ORDER BY/LIMIT. The goal is to add Elasticsearch-like capabilities: inverted index, BM25 scoring, text analysis, MATCH/LIKE syntax, aggregations -- all fitting organically into the existing architecture.

## Critical Files

| File | Role |
|---|---|
| `server/internal/storage/storage.go` | StorageEngine interface, IdentifiedRow type |
| `server/internal/storage/file_storage.go` | JSON file storage, index CRUD, auto-update |
| `server/internal/analyzer/*.go` | NEW: text analysis pipeline |
| `server/internal/index/*.go` | NEW: inverted index + BM25 |
| `server/internal/lexer/lexer.go` | Token types, keywords |
| `server/internal/parser/ast.go` | AST node types |
| `server/internal/parser/parser.go` | Recursive descent parser |
| `server/internal/executor/executor.go` | Command factory |
| `server/internal/executor/commands.go` | All command implementations |
| `server/internal/executor/eval.go` | WHERE expression evaluator |

No changes needed to: client code (C++), main.go, go.mod (zero external deps).

---

## Phase 0: Stable RowIDs

**Problem**: Rows are stored as a flat array. DeleteRows shifts indices, making any persistent index invalid.

**Solution**: Add parallel `row_ids []int64` to `tableDataDisk`. Each row gets a stable auto-increment ID via `next_id`.

### Changes

**`storage/storage.go`**:
- New type: `IdentifiedRow{RowID int64, Data Row}`
- `SelectRows` returns `([]IdentifiedRow, error)` instead of `([]Row, error)`
- `UpdateRows` takes `rowIDs []int64` instead of `indices []int`
- `DeleteRows` takes `rowIDs []int64` instead of `indices []int`

**`storage/file_storage.go`**:
- `tableDataDisk`: add `RowIDs []int64 json:"row_ids,omitempty"`, change `NextID` to `int64`
- `readData`: if `RowIDs` absent/mismatched, synthesize `[1..N]`, set `NextID=N+1` (migration)
- `InsertRows`: assign `NextID` to each new row, increment, append to both `Rows` and `RowIDs`
- `SelectRows`: zip `RowIDs[i]` + coerced `Rows[i]` into `[]IdentifiedRow`
- `UpdateRows`: build `map[int64]struct{}` from rowIDs, scan `data.RowIDs` to find positions, apply updates
- `DeleteRows`: filter both `Rows` and `RowIDs` in tandem, keeping rows not in the delete set

**`executor/commands.go`**:
- `SelectCommand`: iterate `[]IdentifiedRow`, call `evalExpr` with `identRow.Data`
- `UpdateCommand`: collect `identRow.RowID` instead of loop index, pass `[]int64` to storage
- `DeleteCommand`: same -- collect `identRow.RowID`

**Tests**: Update `storage_test.go` and `executor_test.go` to use new signatures. Add test: delete row, insert new row, verify surviving row keeps its original RowID.

---

## Phase 1: Text Analysis Pipeline

New package: `server/internal/analyzer/`

**`analyzer.go`**: Types + interfaces
- `AnalyzedToken{Term string, Position int}`
- `Tokenizer` interface: `Tokenize(string) []AnalyzedToken`
- `TokenFilter` interface: `Filter([]AnalyzedToken) []AnalyzedToken`
- `Analyzer` interface: `Analyze(string) []AnalyzedToken`

**`tokenizer.go`**: `WhitespaceTokenizer` -- split on non-letter/non-digit chars (Unicode-aware)

**`filters.go`**: Three filters:
- `LowercaseFilter`: `strings.ToLower` on each term
- `StopWordFilter`: remove ~30 English stop words (`the, a, an, is, are, was, were, ...`)
- `SuffixStemmer`: strip common suffixes (`ing, ed, es, s, tion, ment, ness, ...`), skip terms < 4 chars

**`standard.go`**: `StandardAnalyzer` = Tokenize -> Lowercase -> StopWords -> Stem

**`analyzer_test.go`**: "The Warriors are running!" -> `[{warrior,1}, {runn,3}]`

---

## Phase 2: Inverted Index Engine

New package: `server/internal/index/`

**`types.go`**: Data structures
- `Posting{RowID int64, Frequency int, Positions []int}`
- `PostingList{DocFreq int, Postings []Posting}`
- `InvertedIndex{Column, TotalDocs, AvgFieldLen, DocLengths map[int64]int, Terms map[string]*PostingList}`
- `SearchResult{RowID int64, Score float64}`

**`inverted.go`**: Core operations
- `NewInvertedIndex(column, analyzer)` constructor
- `AddDocument(rowID, text)`: analyze -> build term freq map -> append postings -> update stats
- `RemoveDocument(rowID)`: scan all posting lists, remove postings, update stats
- `Search(query) []SearchResult`: analyze query -> collect candidate rowIDs -> BM25 score -> sort desc

**`bm25.go`**: BM25 scoring (k1=1.2, b=0.75)
- `idf(docFreq, totalDocs) = ln((N-n+0.5)/(n+0.5) + 1)`
- `scoreBM25(queryTerms, rowID, idx)`: sum IDF * (f*(k1+1))/(f+k1*(1-b+b*|D|/avgdl))

**`persistence.go`**: Save/Load to `_index_{column}.json` (atomic write via temp+rename)

**`index_test.go`**: Add 3 docs, search, verify order. Remove doc, re-search. Persistence round-trip.

---

## Phase 3: Extend Lexer + Parser

### Lexer (`lexer/lexer.go`)

New token types: `TOKEN_MATCH, TOKEN_LIKE, TOKEN_LIMIT, TOKEN_OFFSET, TOKEN_ORDER, TOKEN_BY, TOKEN_ASC, TOKEN_DESC, TOKEN_INDEX, TOKEN_ON, TOKEN_COUNT, TOKEN_SUM, TOKEN_AVG, TOKEN_MIN, TOKEN_MAX, TOKEN_GROUP, TOKEN_AS`

Add to `keywords` map + `String()` method.

### AST (`parser/ast.go`)

New expression types:
- `MatchExpr{Column, Query string}` -- `WHERE MATCH(bio, 'warrior king')`
- `LikeExpr{Column, Pattern string, Negated bool}` -- `WHERE name LIKE '%ara%'`

New statement types:
- `CreateIndexStatement{IndexName, TableName, ColumnName string}`
- `DropIndexStatement{IndexName, TableName string}`

New supporting types:
- `SelectItem{Type string, ColumnName string, AggFunc string, Alias string}` -- replaces `[]string` in select list
- `OrderByItem{Column string, Desc bool}`
- `LimitClause{Count int, Offset int}`

Modify `SelectStatement`:
- `Columns []string` -> `Items []SelectItem`
- Add `GroupBy []string`, `OrderBy []OrderByItem`, `Limit *LimitClause`

### Parser (`parser/parser.go`)

- `parseSelect`: rework column parsing -> `parseSelectItems()`, add parsing for GROUP BY, ORDER BY, LIMIT after WHERE
- `parsePrimary`: handle `TOKEN_MATCH` -> `MatchExpr`
- `parseComparison`: handle `TOKEN_LIKE` after left operand -> `LikeExpr`, `NOT LIKE` -> `LikeExpr{Negated:true}`
- `parseCreate`: add `TOKEN_INDEX` branch -> `CreateIndexStatement`
- `parseDrop`: add `TOKEN_INDEX` branch -> `DropIndexStatement`

### New SQL Syntax

```sql
SELECT * FROM heroes WHERE MATCH(bio, 'warrior king');
SELECT name, _score FROM heroes WHERE MATCH(bio, 'elven') ORDER BY _score DESC;
SELECT * FROM heroes WHERE name LIKE '%ara%';
SELECT * FROM heroes LIMIT 10 OFFSET 5;
SELECT COUNT(*) FROM heroes WHERE alive = TRUE;
SELECT level, COUNT(*) AS cnt FROM heroes GROUP BY level;
CREATE INDEX idx_bio ON heroes(bio);
DROP INDEX idx_bio ON heroes;
```

---

## Phase 4: Storage Integration

**`storage/storage.go`** -- add to interface:
- `CreateIndex(dbName, tableName, indexName, columnName string) error`
- `DropIndex(dbName, tableName, indexName string) error`
- `GetIndex(dbName, tableName, columnName string) (*index.InvertedIndex, error)`
- `SaveIndex(dbName, tableName, columnName string, idx *index.InvertedIndex) error`
- `ListIndexes(dbName, tableName string) ([]string, error)`

**`storage/file_storage.go`**:
- `CreateIndex`: validate column is TEXT/VARCHAR, full rebuild from existing data, save to `_index_{col}.json`, register in `_indexes.json`
- `DropIndex`: delete `_index_{col}.json`, remove from `_indexes.json`
- Auto-update hooks at end of `InsertRows`, `UpdateRows`, `DeleteRows`: for each indexed column, load index, add/remove/update documents, save

**Locking**: Index mutations happen within existing table write lock. No extra locks needed.

---

## Phase 5: Search Commands in Executor

### New Query Pipeline for SelectCommand

```
Load IdentifiedRows -> Pre-compute MATCH scores -> Filter WHERE -> GROUP BY -> ORDER BY -> LIMIT -> Project
```

### Key Implementations

**MATCH pre-computation**: Walk WHERE tree, find MatchExpr nodes, load indexes, run `index.Search(query)` -> `map[int64]float64` (rowID -> score)

**EvalContext**: New struct `{RowID int64, MatchScores map[string]float64}` passed to `evalExpr` for MATCH evaluation

**evalExpr changes**: Accept `*EvalContext`, handle `MatchExpr` (lookup score > 0) and `LikeExpr` (pattern matching with `%` and `_`)

**LIKE matching**: `%` = any chars, `_` = single char. Simple iterative matcher, not regex.

**_score virtual column**: In projection, if SelectItem is `_score`, look up cumulative MATCH score for that row. No MATCH -> `"0"`.

**ORDER BY**: `sort.SliceStable` with multi-column comparator using existing `compareValues` logic. `_score` resolved from match context.

**LIMIT/OFFSET**: Simple slice after sorting.

**CreateIndexCommand / DropIndexCommand**: Delegate to storage, return success message.

---

## Phase 6: Aggregations

New file: `server/internal/executor/aggregate.go`

**Aggregate functions**: COUNT(*), SUM(col), AVG(col), MIN(col), MAX(col)
- NULL values skipped for SUM/AVG/MIN/MAX
- SUM/AVG error on non-numeric columns

**Without GROUP BY**: All filtered rows = single group, return 1 result row
**With GROUP BY**: Partition rows by group key values, compute per group

**Validation**: Non-aggregate SELECT columns must appear in GROUP BY list.

**Result column naming**: `COUNT(*)`, `SUM(level)` etc., or alias if specified (`AS total`).

---

## Phase 7: Integration Testing

New file: `server/internal/executor/integration_test.go`

Key scenarios:
1. Full lifecycle: create table -> insert -> create index -> MATCH search -> insert more -> search again -> delete -> search -> drop index -> verify MATCH errors
2. Complex query: `SELECT name, _score FROM t WHERE MATCH(body, 'dragon') AND year > 2020 ORDER BY _score DESC LIMIT 5;`
3. LIKE + ORDER BY + LIMIT: `SELECT * FROM heroes WHERE name LIKE 'A%' ORDER BY level DESC LIMIT 1;`
4. Backward compat: legacy `_data.json` (no row_ids) transparently migrates on first mutation
5. Index consistency through mutations: insert 100 rows, delete half, update some, search -- always consistent
6. Edge cases: empty table, LIMIT 0, _score without MATCH, multiple indexes on same table

---

## Verification

After all phases:
```bash
cd server && GOCACHE=/tmp/go-cache GOMODCACHE=/tmp/go-mod-cache go test ./...
```

All existing tests must pass (backward compat). All new tests must pass. Zero external dependencies added.
