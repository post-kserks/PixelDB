# GUIDE

This guide explains how to work with PixelDB: startup, schema design, data changes, full-text search, and aggregations.

## 1. Start PixelDB

Build:

```bash
./build.sh
```

Run server:

```bash
./run.sh
```

Run client shell:

```bash
./build/pixeldb-shell 127.0.0.1 5432
```

You can type SQL directly in the shell. If you do not end a query with `;`, the shell appends it automatically.

## 2. Basic Workflow

### 2.1 Create and use a database

```sql
CREATE DATABASE game;
USE game;
```

### 2.2 Create tables

Supported column types:

- `INT`
- `FLOAT`
- `BOOL`
- `TEXT`
- `VARCHAR(n)`

Example:

```sql
CREATE TABLE heroes (
  id INT,
  name VARCHAR(100),
  level INT,
  alive BOOL,
  score FLOAT,
  bio TEXT,
  year INT
);
```

### 2.3 Insert data

```sql
INSERT INTO heroes VALUES (1, 'Aragorn', 10, TRUE, 9.8, 'Warrior king of Gondor', 2022);
INSERT INTO heroes (id, name, bio) VALUES (2, 'Legolas', 'Elven archer of Mirkwood');
```

### 2.4 Read data

```sql
SELECT * FROM heroes;
SELECT id, name FROM heroes WHERE level >= 9 AND alive = TRUE;
```

### 2.5 Update and delete

```sql
UPDATE heroes SET level = 11 WHERE id = 1;
DELETE FROM heroes WHERE alive = FALSE;
```

## 3. Pattern Search (`LIKE`)

`LIKE` supports:

- `%` - any sequence of characters
- `_` - exactly one character

Examples:

```sql
SELECT * FROM heroes WHERE name LIKE 'A%';
SELECT * FROM heroes WHERE name LIKE '%or%';
SELECT * FROM heroes WHERE name NOT LIKE '%orc%';
```

## 4. Full-Text Search (`MATCH`)

### 4.1 Create index

Full-text index can be created only on `TEXT`/`VARCHAR` columns.

```sql
CREATE INDEX idx_heroes_bio ON heroes(bio);
```

### 4.2 Search with MATCH

```sql
SELECT * FROM heroes WHERE MATCH(bio, 'warrior king');
```

### 4.3 Relevance score

`_score` contains BM25 score.

```sql
SELECT name, _score
FROM heroes
WHERE MATCH(bio, 'elven archer')
ORDER BY _score DESC;
```

Notes:

- If `MATCH` is used and no index exists for that column, query returns an error.
- If `_score` is selected without any `MATCH`, `_score` is `0`.

### 4.4 Drop index

```sql
DROP INDEX idx_heroes_bio ON heroes;
```

## 5. Sorting and Pagination

```sql
SELECT id, name, level
FROM heroes
ORDER BY level DESC, name ASC
LIMIT 10 OFFSET 20;
```

Rules:

- `OFFSET` is supported only with `LIMIT`.
- `LIMIT 0` returns an empty result set.

## 6. Aggregations

Supported aggregate functions:

- `COUNT(*)`
- `SUM(column)`
- `AVG(column)`
- `MIN(column)`
- `MAX(column)`

### 6.1 Without GROUP BY

```sql
SELECT COUNT(*) AS total FROM heroes WHERE alive = TRUE;
SELECT AVG(level) AS avg_level FROM heroes;
```

### 6.2 With GROUP BY

```sql
SELECT level, COUNT(*) AS cnt
FROM heroes
GROUP BY level
ORDER BY cnt DESC;
```

Rules:

- Non-aggregate selected columns must be included in `GROUP BY`.
- `SUM`/`AVG` require numeric columns.
- `NULL` values are skipped in `SUM`/`AVG`/`MIN`/`MAX`.

## 7. Typical End-to-End Example

```sql
CREATE DATABASE demo;
USE demo;

CREATE TABLE docs (
  id INT,
  title TEXT,
  body TEXT,
  year INT
);

INSERT INTO docs VALUES
  (1, 'A', 'dragon rider from the north', 2021),
  (2, 'B', 'elven archer in forest', 2020),
  (3, 'C', 'ancient dragon king', 2024);

CREATE INDEX idx_docs_body ON docs(body);

SELECT title, _score
FROM docs
WHERE MATCH(body, 'dragon king') AND year > 2020
ORDER BY _score DESC
LIMIT 5;

SELECT year, COUNT(*) AS cnt
FROM docs
GROUP BY year
ORDER BY year ASC;
```

## 8. Storage Notes

- Data is stored in JSON files under `data/databases/...`.
- Rows have stable internal RowID values (used by index subsystem).
- Legacy tables without `row_ids` are migrated automatically on first mutation.

## 9. Troubleshooting

- `table 'X' does not exist`:
  - check `USE <db>;`
  - check table name
- `MATCH(column, ...): index for column ... does not exist`:
  - create full-text index with `CREATE INDEX`
- parser errors:
  - check semicolon
  - check clause order (`WHERE` -> `GROUP BY` -> `ORDER BY` -> `LIMIT/OFFSET`)
