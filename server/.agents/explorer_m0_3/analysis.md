# Analysis Report: VaultDB Extreme Concurrency Test Requirements (R3)

**Author:** Explorer 3  
**Target:** VaultDB Chaos Test Suite — Requirement R3 (`TestChaosHighConcurrency`)  
**Working Directory:** `/Users/xserx/projects/pro-labs/server/.agents/explorer_m0_3`  
**Project Root:** `/Users/xserx/projects/pro-labs/server`  

---

## 1. Requirement & Scope Analysis (R3)

### 1.1 Objective
Requirement R3 demands verifying that VaultDB can withstand extreme concurrent access patterns without deadlocks, data races, unhandled panics, or storage corruption.

### 1.2 Key Specifications & Acceptance Criteria
1. **Goroutine Volume**: Spawn **500+ concurrent worker goroutines** executing queries in parallel.
2. **Workload Diversity**: Perform mixed operations:
   - `SELECT` (point queries & aggregate/range queries)
   - `UPDATE` (in-place row value & counter updates)
   - `DELETE` (tuple deletions / dead slot marking)
   - `VACUUM` (table-level page compaction & shadow file directory replacement)
3. **Execution Control**:
   - Proper context cancellation (`context.WithTimeout`).
   - Synchronization via `sync.WaitGroup`.
   - Clean handling of expected transaction conflicts (OCC rollback).
4. **Safety & Quality**:
   - Zero data races: Must pass Go race detector (`go test -race`).
   - Zero deadlocks or goroutine leaks.
   - Zero unhandled panics.
5. **Build Tag Uniformity**:
   - All chaos test files must contain `//go:build chaos` tag.
   - Full suite execution: `go test -tags=chaos ./...`.

---

## 2. Codebase Inspection & Architectural Findings

### 2.1 Query Execution & Session Management
- **Files Inspected**:
  - `internal/core/executor/executor.go`
  - `internal/core/executor/session.go` (lines 29-122)
  - `internal/core/executor/session_registry.go` (lines 38-126)
- **Observations**:
  - `executor.Executor` executes parsed SQL statements against `storage.StorageEngine` and `txmanager.Manager`.
  - `Session` holds state specific to a connection: `currentDB`, `ActiveTx`, `Variables`, `PreparedStatements`, and session mutex `s.mu`.
- **Concurrency & Session Isolation Requirement**:
  - *Observation*: `Session.ActiveTx` and `Session.currentDB` are modified during statement execution.
  - *Deduction*: Sharing a single `Session` across 500+ concurrent goroutines causes session-level state conflicts (e.g. goroutine A overwriting `ActiveTx` set by goroutine B).
  - *Design*: Each worker goroutine in `TestChaosHighConcurrency` **must instantiate its own `Session`** (`s := executor.NewSession(store, metrics.New(), txm, nil)` or `executor.NewSessionWithConfig(...)`) and run `s.SetCurrentDatabase("chaosdb")`. All worker sessions connect to the same underlying `PageStorageEngine` and `txmanager.Manager`.

### 2.2 Storage Engine & Vacuum Mechanism
- **Files Inspected**:
  - `internal/core/storage/page_engine.go` (lines 41-102)
  - `internal/core/storage/page_engine_vacuum.go` (lines 18-184)
  - `internal/core/storage/vacuum.go` (lines 18-172)
  - `internal/core/storage/buffer_pool.go` (lines 351-368)
- **Observations**:
  - `PageStorageEngine` uses per-table locks `table.mu` (`LWRLock`).
  - `Vacuum(dbName, tableName)` workflow:
    1. Acquires `e.mu` briefly to get table handle `t`.
    2. Acquires `t.mu.Lock()` (exclusive table write lock).
    3. Flushes dirty pages from buffer pool via `e.bufPool.FlushAll()`.
    4. Iterates through all heap pages in `t.heap`, filters out dead tuples (where `deletedTx` is committed and older than active transactions), and rebuilds live pages into `.vacuum` shadow heap file.
    5. Closes `t.heap`, removes original directory, renames shadow directory to original directory, and re-opens `t.heap = newHF`.
    6. Calls `e.bufPool.InvalidateTable(t.tableID)` to clear unpinned cached pages for that table.
    7. Updates table catalog metadata under `e.mu`.
- **Concurrency & Deadlock Analysis**:
  - `Vacuum` holds `t.mu.Lock()` exclusively while creating and substituting the shadow table file.
  - Concurrent `SELECT`, `UPDATE`, `INSERT`, and `DELETE` operations acquire `t.mu.RLock()` or `t.mu.Lock()`.
  - When `Vacuum` is executing, DML goroutines targeting the same table block cleanly on `t.mu` spinlock/channel.
  - No lock ordering inversion exists between `e.mu`, `t.mu`, and `bufPool.mu` as `e.mu` is never acquired while holding `t.mu` except during short catalog updates.

### 2.3 Transaction Engine & OCC Conflict Handling
- **Files Inspected**:
  - `internal/core/txmanager/manager.go` (lines 121-200)
- **Observations**:
  - `txmanager.Manager` uses Optimistic Concurrency Control (OCC) with table versioning (`BumpTableVersion`).
  - Transactions record read/write operations. Upon `Commit`, table versions are validated. If a table modified by transaction T was altered by another transaction since T started, `m.Commit` returns `ErrTxConflict`.
- **Deduction for Test Design**:
  - Under 500+ concurrent workers updating the same table, OCC transaction conflicts will occur naturally.
  - The chaos test worker loop must catch `ErrTxConflict` or error strings containing `"conflict"` / `"transaction conflict"` / `"duplicate"` and treat them as normal OCC behavior (either roll back & retry or count as expected conflict).

### 2.4 Build Tags Audit
- **Files Inspected**:
  - `internal/core/wal/chaos_test.go`
  - `internal/core/executor/chaos_test.go`
  - `internal/core/txmanager/chaos_test.go`
- **Observations**:
  - `internal/core/wal/chaos_test.go`: Line 1 contains `//go:build chaos` ✅
  - `internal/core/executor/chaos_test.go`: Line 1 is `package executor` (NO build tag!) ❌
  - `internal/core/txmanager/chaos_test.go`: Line 1 is `package txmanager` (NO build tag!) ❌
- **Resolution**:
  - Add `//go:build chaos` at line 1 of `internal/core/executor/chaos_test.go` and `internal/core/txmanager/chaos_test.go`.
  - All chaos tests can then be run together using `go test -buildvcs=false -tags=chaos ./...`.

---

## 3. Recommended Implementation Strategy for `TestChaosHighConcurrency`

### 3.1 Test Location & Signature
- **Location**: `internal/core/executor/chaos_test.go`
- **Build Tag**: `//go:build chaos`
- **Signature**: `func TestChaosHighConcurrency(t *testing.T)`

### 3.2 Detailed Design

```
                     ┌────────────────────────────────────────┐
                     │       TestChaosHighConcurrency         │
                     │          (500+ Goroutines)             │
                     └──────────────────┬─────────────────────┘
                                        │
             ┌──────────────────────────┼──────────────────────────┐
             ▼                          ▼                          ▼
   ┌──────────────────┐       ┌──────────────────┐       ┌──────────────────┐
   │ 200 Reader Workers│       │150 Updater Workers│       │75 Inserter Workers│
   │  SELECT queries  │       │  UPDATE queries  │       │  INSERT queries  │
   └─────────┬────────┘       └─────────┬────────┘       └─────────┬────────┘
             │                          │                          │
             └──────────────────────────┼──────────────────────────┘
                                        │
             ┌──────────────────────────┴──────────────────────────┐
             ▼                                                     ▼
   ┌──────────────────┐                                  ┌──────────────────┐
   │ 70 Deleter Workers│                                  │25 Vacuumer Workers│
   │  DELETE queries  │                                  │  VACUUM queries  │
   └─────────┬────────┘                                  └─────────┬────────┘
             │                                                     │
             └──────────────────────────┬──────────────────────────┘
                                        │
                                        ▼
                      ┌───────────────────────────────────┐
                      │    Per-Worker Dedicated Session   │
                      │  Shared Storage Engine & TxManager│
                      └───────────────────────────────────┘
```

### 3.3 Goroutine Allocation Matrix
| Worker Role | Goroutine Count | Executed Operations | Expected Result / Handling |
|-------------|-----------------|---------------------|----------------------------|
| **Readers** | 200 | `SELECT * FROM chaos_table WHERE id = ?;`<br>`SELECT COUNT(*) FROM chaos_table;` | Always succeeds or returns standard rows |
| **Updaters** | 150 | `UPDATE chaos_table SET counter = counter + 1, val = ? WHERE id = ?;` | May encounter OCC conflict; rollback/retry |
| **Inserters** | 75 | `INSERT INTO chaos_table VALUES (?, ?, ?);` | May encounter duplicate key error; ignore duplicate errors |
| **Deleters** | 70 | `DELETE FROM chaos_table WHERE id = ?;` | May delete 0 rows or encounter OCC conflict; handled cleanly |
| **Vacuumers** | 25 | `VACUUM chaos_table;` | Compacts table heap under `t.mu.Lock()`; blocks DML cleanly |
| **Total** | **520** | Mixed concurrent DML + DDL | **Zero races, deadlocks, or panics** |

### 3.4 Context Cancellation & Timeout Pattern
```go
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()

var wg sync.WaitGroup
var totalOps, conflictOps, errOps atomic.Int64

for i := 0; i < numWorkers; i++ {
    wg.Add(1)
    go func(workerID int, role string) {
        defer wg.Done()
        
        // Dedicated session per worker goroutine
        sess := executor.NewSession(store, metrics.New(), txm, nil)
        sess.SetCurrentDatabase("chaosdb")
        
        for {
            select {
            case <-ctx.Done():
                return
            default:
                // execute role-specific query
                // update atomic metrics
            }
        }
    }(i, roles[i])
}

wg.Wait()
```

### 3.5 Verification Criteria & Race Detector
1. **Command**: `go test -buildvcs=false -race -tags=chaos ./internal/core/executor -run TestChaosHighConcurrency`
2. **Data Race Check**: `-race` output must report zero data races.
3. **Deadlock & Leak Check**: `wg.Wait()` completes cleanly within the context window plus a small buffer (no blocked goroutines).
4. **Data Integrity Check**: After workers complete, run `SELECT COUNT(*) FROM chaos_table;` to verify database remains readable and healthy.

---

## 4. Summary Table of Actionable Next Steps

| Item | File | Action Required | Responsible Milestone |
|------|------|-----------------|-----------------------|
| 1 | `internal/core/executor/chaos_test.go` | Add `//go:build chaos` tag at top of file | M3 / Implementer |
| 2 | `internal/core/txmanager/chaos_test.go` | Add `//go:build chaos` tag at top of file | M3 / Implementer |
| 3 | `internal/core/executor/chaos_test.go` | Implement full `TestChaosHighConcurrency` with 520 goroutines (`SELECT`, `UPDATE`, `DELETE`, `VACUUM`) | M3 / Implementer |
| 4 | Test Suite Verification | Run `go test -buildvcs=false -race -tags=chaos ./...` | M4 / Final Verification |
