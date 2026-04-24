# PixelDB

PixelDB is an educational SQL database with a retro RPG terminal style.

## Components

- `server/` — Go TCP server with SQL lexer/parser, command executor, and JSON file storage.
- `client/` — C++17 shared library (`libpixeldb`) and interactive shell (`pixeldb-shell`).

## Supported SQL

- DDL: `CREATE DATABASE`, `DROP DATABASE`, `CREATE TABLE`, `DROP TABLE`, `USE`
- DML: `SELECT`, `INSERT`, `UPDATE`, `DELETE`
- `WHERE` expressions with `AND`, `OR`, `NOT`, parentheses, and comparison operators
- Data types: `INT`, `FLOAT`, `BOOL`, `TEXT`, `VARCHAR(n)`

## Build

```bash
./build.sh
```

Artifacts are placed into `build/`:

- `build/pixeldb-server`
- `build/libpixeldb*`
- `build/pixeldb-shell`

## Run server

```bash
./run.sh
./run.sh 0.0.0.0 7777
```

## Run shell client

```bash
./build/pixeldb-shell
./build/pixeldb-shell 127.0.0.1 5432
```

## Tests

```bash
cd server
GOCACHE=/tmp/go-cache GOMODCACHE=/tmp/go-mod-cache go test ./...
```
