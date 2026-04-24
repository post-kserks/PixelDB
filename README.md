# PixelDB

PixelDB is an educational SQL database with a Go server, a C++17 client, JSON file storage, and Elasticsearch-like full-text search features.

## What Is Included

- `server/` - TCP server (Go) with SQL lexer/parser, executor, storage engine, text analyzer, and inverted index.
- `client/` - C++17 library (`libpixeldb`) and interactive shell (`pixeldb-shell`).
- `data/` - runtime data directory for databases and tables.

## Supported SQL Features

### Core SQL

- DDL:
  - `CREATE DATABASE`
  - `DROP DATABASE`
  - `USE`
  - `CREATE TABLE`
  - `DROP TABLE`
- DML:
  - `SELECT`
  - `INSERT`
  - `UPDATE`
  - `DELETE`

### Query Language

- `WHERE` with `AND`, `OR`, `NOT`, parentheses, and operators: `=`, `!=`, `<`, `>`, `<=`, `>=`
- `LIKE` and `NOT LIKE` (`%` and `_` patterns)
- `ORDER BY ... ASC|DESC`
- `LIMIT` and `OFFSET`

### Full-Text Search

- `CREATE INDEX idx_name ON table_name(text_column)`
- `DROP INDEX idx_name ON table_name`
- `MATCH(column, 'query text')` in `WHERE`
- BM25 scoring
- `_score` virtual projection column

### Aggregations

- `COUNT(*)`
- `SUM(column)`
- `AVG(column)`
- `MIN(column)`
- `MAX(column)`
- `GROUP BY`

## Build

```bash
./build.sh
```

Build artifacts are produced in `build/`:

- `build/pixeldb-server`
- `build/pixeldb-shell`
- `build/libpixeldb*`

## Run Server

```bash
./run.sh
./run.sh 0.0.0.0 7777
```

Server flags (from binary):

- `-host` (default `127.0.0.1`)
- `-port` (default `5432`)
- `-data` (default `./data`)

## Run Shell Client

```bash
./build/pixeldb-shell
./build/pixeldb-shell 127.0.0.1 5432
```

Client commands:

- type SQL queries
- `exit` or `quit` to close shell

## Tests

```bash
cd server
GOCACHE=/tmp/go-cache GOMODCACHE=/tmp/go-mod-cache go test ./...
```

## Documentation

- Detailed usage guide: `GUIDE.md`
- Project change roadmap: `ROADMAP.md`
