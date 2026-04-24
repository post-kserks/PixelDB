# ROADMAP

This document describes the implemented evolution of PixelDB from a basic educational SQL engine to a search-capable system with Elasticsearch-like features.

## Baseline (Before Changes)

Initial state:

- JSON file storage without indexing.
- Every `SELECT` scanned full table data.
- No full-text search, no relevance scoring.
- No `ORDER BY`, `LIMIT/OFFSET`, or aggregations.
- Basic SQL support: core DDL/DML and boolean/comparison expressions.

## Phase 0 - Stable RowIDs

Implemented:

- Stable row identifiers (`RowID`) that survive delete operations.
- Storage API switched from row positions to `rowIDs` for update/delete.
- Data format supports `row_ids` and `next_id`.
- Automatic migration for legacy `_data.json` without `row_ids`.

Why:

- Persistent indexes require stable document identifiers.

## Phase 1 - Text Analysis Pipeline

Implemented new package: `server/internal/analyzer/`.

Features:

- Token stream model with token positions.
- Unicode-aware tokenizer (split by non-letter/non-digit boundaries).
- Filters:
  - lowercase normalization
  - stop-word removal
  - lightweight suffix stemming
- Standard analyzer pipeline (`Tokenizer -> Lowercase -> StopWords -> Stem`).

Why:

- Full-text search quality depends on normalized term extraction.

## Phase 2 - Inverted Index + BM25

Implemented new package: `server/internal/index/`.

Features:

- Inverted index data structures (`Posting`, `PostingList`, `InvertedIndex`).
- Document length tracking and average field length.
- BM25 scoring (`k1=1.2`, `b=0.75`).
- Full-text search API returning ordered `(RowID, Score)` results.
- Persistence (save/load JSON files with atomic write).

Why:

- Provides scalable full-text retrieval instead of table-wide scans.

## Phase 3 - Lexer/Parser Extensions

Implemented:

- New SQL keywords and tokens for search/sort/aggregation/index DDL.
- New AST nodes:
  - `MatchExpr`
  - `LikeExpr`
  - `CreateIndexStatement` / `DropIndexStatement`
  - richer `SelectStatement` with select items, grouping, sorting, pagination
- Parser support for:
  - `MATCH(column, 'query')`
  - `LIKE` / `NOT LIKE`
  - `GROUP BY`
  - `ORDER BY`
  - `LIMIT ... OFFSET ...`
  - aggregate projections
  - index creation/removal

Why:

- Required to expose search and analytics capabilities through SQL.

## Phase 4 - Storage Integration for Indexes

Implemented in `FileStorageEngine`:

- Extended storage interface with index lifecycle methods:
  - `CreateIndex`
  - `DropIndex`
  - `GetIndex`
  - `SaveIndex`
  - `ListIndexes`
- Index metadata registry via `_indexes.json`.
- Per-column index files `_index_{column}.json`.
- Full index rebuild on creation.
- Automatic index sync after `INSERT/UPDATE/DELETE` under existing table write lock.

Why:

- Keeps search index consistent with table data.

## Phase 5 - Executor Search Pipeline

Implemented in executor:

- New `SELECT` pipeline:
  - load rows
  - precompute MATCH scores from indexes
  - evaluate WHERE (including MATCH/LIKE)
  - apply grouping/aggregation if requested
  - sort
  - apply limit/offset
  - project selected fields
- `_score` virtual column support.
- `CREATE INDEX` / `DROP INDEX` commands in command factory.
- LIKE matcher implemented as iterative wildcard engine (`%`, `_`).

Why:

- Integrates new parser and storage capabilities into query execution flow.

## Phase 6 - Aggregations

Implemented in `executor/aggregate.go`:

- Aggregate functions:
  - `COUNT(*)`
  - `SUM(column)`
  - `AVG(column)`
  - `MIN(column)`
  - `MAX(column)`
- Grouping via `GROUP BY`.
- Validation rules for aggregate/non-aggregate projection combinations.
- Ordering and pagination over aggregated result sets.
- `NULL` handling:
  - skipped in `SUM/AVG/MIN/MAX`

Why:

- Adds analytical query capabilities.

## Phase 7 - Test Coverage and Integration Validation

Added and updated tests across modules:

- Analyzer tests.
- Inverted index tests (search order, deletion, persistence round-trip).
- Lexer/parser tests for new SQL syntax.
- Storage tests for index lifecycle, auto-sync, legacy migration.
- Executor unit and integration tests:
  - full-text lifecycle
  - complex MATCH filters + ordering + limit
  - LIKE + ORDER BY + LIMIT
  - aggregation paths
  - backward compatibility checks

Result:

- `go test ./...` in `server/` passes.

## Current Status

Implemented and working:

- Stable RowID model.
- Full-text indexing and BM25 search.
- `MATCH`, `LIKE`, `ORDER BY`, `LIMIT/OFFSET`.
- Aggregations with `GROUP BY`.
- Index DDL and persistent index storage.
- Automatic index consistency on data mutations.

## Possible Next Steps

Potential future improvements:

- Incremental index update logic (instead of rebuild-on-mutation strategy).
- Per-index analyzer configuration.
- Additional query features (`HAVING`, multi-field ranking strategies).
- Extended aggregation types (bucket/terms-like analytics).
- Performance instrumentation and query planner heuristics.
