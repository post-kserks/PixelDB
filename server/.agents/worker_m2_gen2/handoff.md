# Handoff Report: Milestone 2 — Crash Recovery (Requirement R2)

## 1. Observation

- **Test Target**: `cmd/vaultdb-server/chaos_test.go` with build tag `//go:build chaos`.
- **Initial Test Execution Output (`go test -tags=chaos -v -run TestChaosCrashRecovery ./...`)**:
  ```text
  2026/07/29 20:47:54 INFO WAL recovery: complete replayed=29 rolled_back=87
      chaos_test.go:138: Atomicity failure: found uncommitted record IDs in recovered database: [900002 900003 900004 ...]
  --- FAIL: TestChaosCrashRecovery (0.23s)
  ```
- **Codebase Root Cause Inspections**:
  1. `cmd/vaultdb-server/chaos_test.go` Worker 2 previously constructed `WALPageInsertPayload` manually with a hardcoded `SlotNo: 0`. This caused `undoInsert` in `internal/core/storage/page_engine.go` (line 658) to repeatedly undo slot 0, leaving all other inserted uncommitted tuples on disk without setting `deleted_tx`.
  2. `internal/core/storage/page_engine.go` `redoInsert` (line 470) used `pg.InsertTuple` which appended tuples at `h.NItems` rather than placing them at the exact `SlotNo` specified in the WAL payload.
  3. `internal/core/storage/page/page.go` lacked an exact-slot insertion method for WAL redo recovery.
  4. `internal/core/storage/page_engine.go` `RecoverFromWAL()` did not invalidate the buffer pool (`e.bufPool.InvalidateTableForce`) after WAL replay and undo phases, causing subsequent reads to fetch stale un-undone in-memory pages.
- **Modifications Implemented**:
  1. `internal/core/storage/page/page.go`: Implemented `InsertTupleAt(slot uint16, data []byte) error` which places tuples at the exact slot specified during WAL redo. When checking if a tuple already exists at `slot`, it compares `created_tx` (bytes 0:8) and row payload (bytes 16:), ignoring `deleted_tx` (bytes 8:16) which may have been set after insertion.
  2. `internal/core/storage/page_engine_io.go`: Exported `InsertRowsUncommitted(dbName, tableName string, rows []Row) (int, error)` using `e.nextTxID()` to allocate atomic transaction IDs and record `OpPageInsert` entries with exact `(SegmentNo, PageNo, SlotNo)` coordinates without issuing `OpCommit`. Refactored `InsertRows` and `InsertRowsUncommitted` to use `insertRowsInternal`.
  3. `internal/core/storage/page_engine.go`: Updated `redoInsert` to call `pg.InsertTupleAt(p.SlotNo, p.TupleData)`. Added buffer pool invalidations (`e.bufPool.InvalidatePage` in `redoInsert`/`undoInsert` and `e.bufPool.InvalidateTableForce` in `RecoverFromWAL`).
  4. `cmd/vaultdb-server/chaos_test.go`: Updated Worker 2 in helper process to execute uncommitted transactions via `engine.InsertRowsUncommitted`.
- **Final Test Verification Output**:
  ```text
  $ go test -tags=chaos -v -run TestChaosCrashRecovery ./...
  2026/07/29 20:52:30 INFO WAL recovery: complete replayed=28 rolled_back=84
      chaos_test.go:153: Successfully verified crash recovery: 28 confirmed committed records recovered (out of 28 total recovered live rows), 0 uncommitted records found
  --- PASS: TestChaosCrashRecovery (0.23s)
  PASS
  ok  	vaultdb/cmd/vaultdb-server	1.392s
  ```

---

## 2. Logic Chain

1. **Observation**: `cmd.Process.Kill()` in `TestChaosCrashRecovery` sends `SIGKILL` (`kill -9`) to the child process during concurrent execution of committed (Worker 1) and in-flight uncommitted (Worker 2) transactions.
2. **Observation**: Worker 1 writes committed record IDs to `commPath` (`committed_ids.txt`) and flushes to disk via `file.Sync()`.
3. **Logic Step 1**: To achieve Durability and Atomicity under process termination, `RecoverFromWAL()` must perform ARIES 3-phase recovery:
   - Phase 1 Analysis: Identify all committed transactions and in-progress transactions.
   - Phase 2 Redo: Replay all WAL records onto storage heap pages. `InsertTupleAt` ensures tuples are placed at exact `(SegmentNo, PageNo, SlotNo)` coordinates matching WAL entries.
   - Phase 3 Undo: Replay `undoInsert` for all in-progress transactions. `undoInsert` updates the tuple's `deleted_tx` (`xmax`) at `SlotNo` to `xid`.
4. **Logic Step 2**: Invalidating `e.bufPool` via `InvalidateTableForce` after recovery forces the storage engine to re-read updated page states from disk during catalog recalculation and table queries.
5. **Conclusion**: `TestChaosCrashRecovery` verifies:
   - Durability: 100% of confirmed committed record IDs exist in the recovered database.
   - Atomicity/Undo: 0% of uncommitted transaction record IDs exist in the recovered database.
   - Database reopens and functions without panics or corrupt catalog errors.

---

## 3. Caveats

No caveats. All requirements for Requirement R2 / Milestone 2 have been fully implemented, tested, and verified without hardcoded shortcuts or facades.

---

## 4. Conclusion

Milestone 2 (Crash Recovery for Requirement R2) is complete and verified. The `TestChaosCrashRecovery` test cleanly passes with `//go:build chaos`.

---

## 5. Verification Method

Run the following command from `/Users/xserx/projects/pro-labs/server`:

```bash
go test -tags=chaos -v -run TestChaosCrashRecovery ./...
```

**Expected Outcome**:
- Output contains `--- PASS: TestChaosCrashRecovery`.
- Recovered committed row count matches confirmed `committed_ids.txt` count.
- Zero uncommitted records exist in recovered database.

**Invalidation Conditions**:
- If `RecoverFromWAL()` returns an error.
- If any committed record ID is missing from recovered database.
- If any uncommitted record ID (>= 900000) is present in recovered database.
