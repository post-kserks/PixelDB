# Handoff Report: VaultDB Crash Recovery (Requirement R2)

## 1. Observation

- **Server Entry Point (`cmd/vaultdb-server/main.go`)**:
  - `main()` parses flags: `-host`, `-port`, `-http-port`, `-monitor-port`, `-data`, `-config`. (Lines 180–190)
  - `setupStorage()` opens WAL at `filepath.Join(dataDir, "wal", "vaultdb.wal")`, initializes `storage.NewPageStorageEngine()`, and immediately runs `pageStore.RecoverFromWAL()`. (Lines 77–96)
  - PGWire TCP server is initialized via `pgwire.NewServer()` listening on TCP port. (Lines 356–366)
- **WAL & ARIES Recovery (`internal/core/storage/page_engine.go`)**:
  - `RecoverFromWAL()` (Lines 203–280) performs ARIES 3-Phase recovery:
    1. Phase 1 Analysis: Calls `e.wal.AnalyzeTransactions()` returning `committed` map and `inProgress` map.
    2. Phase 2 Redo: Replays all WAL entries via `redoPhase()`.
    3. Phase 3 Undo: Rolls back uncommitted `inProgress` transactions via `undoPhase()`.
    4. Syncs heap files, recalculates catalog, writes WAL `OpCheckpoint` record, and saves catalog LSN.
- **WAL Operation Types (`internal/core/wal/wal.go`)**:
  - `OpCommit` (`0x50`): Logged when a transaction commits.
  - `OpAbort` (`0x40`): Logged when a transaction aborts.
  - `OpPageInsert` (`0x20`): Logged when a tuple is inserted into a page.
- **Existing Test Patterns (`internal/core/wal/chaos_test.go` and `internal/core/storage/crash_test.go`)**:
  - `internal/core/wal/chaos_test.go` (Lines 97–144) demonstrates the `os.Args[0]` helper process pattern (`GO_WANT_HELPER_PROCESS=1`) where a parent test process starts a child process, lets it execute operations, kills it with `cmd.Process.Kill()` (`SIGKILL` / `kill -9`), and then recovers the WAL.
  - `internal/core/storage/crash_test.go` (Lines 17–91) demonstrates reopening `PageStorageEngine` on a crashed directory and calling `RecoverFromWAL()` to verify row count durability.

---

## 2. Logic Chain

1. **Observation**: `cmd/vaultdb-server/main.go` uses `RecoverFromWAL()` on startup to restore database state after non-graceful exits.
2. **Observation**: ARIES recovery classifies transactions into `committed` and `inProgress` based on `OpCommit` WAL records.
3. **Logic Step 1**: To test process crash recovery (R2), we must simulate an abrupt `kill -9` (`SIGKILL`) while transactions are actively being processed, ensuring no graceful shutdown (`FinalCheckpoint()`) occurs.
4. **Logic Step 2**: Spawning a child database process via `os.Args[0]` (`GO_WANT_HELPER_PROCESS=1`) enables full isolation of the process memory space and file descriptors, allowing the parent process to send `syscall.SIGKILL` safely without terminating the test runner itself.
5. **Logic Step 3**: The child helper process must execute committed transactions (logging committed row IDs to an `fsync`-ed comm file) alongside active uncommitted transactions.
6. **Logic Step 4**: After sending `kill -9` to the child process, the parent process reopens the database directory using `storage.NewPageStorageEngine` and `RecoverFromWAL()`.
7. **Conclusion**: Verification passes if and only if:
   - All committed row IDs exist in the recovered database (Durability).
   - No uncommitted row IDs exist in the recovered database (Atomicity/Undo).
   - Recovery completes without error or panic.

---

## 3. Caveats

- **Scope Boundary**: This analysis was performed in read-only mode during Exploration Milestone M0. No production code changes were made.
- **Signal Compatibility**: `cmd.Process.Kill()` sends `SIGKILL` on Unix systems (macOS/Linux) and `TerminateProcess` on Windows.
- **FSync Sensitivity**: In the helper child process, the comm file recording committed transaction IDs MUST be `fsync`-ed (`f.Sync()`) after writing each commit confirmation to avoid OS-level buffer caching discrepancies during `kill -9`.

---

## 4. Conclusion

Requirement R2 (`TestChaosCrashRecovery`) can be cleanly implemented as a self-contained chaos test under `//go:build chaos`.

Key implementation components for Implementer:
1. File: `cmd/vaultdb-server/chaos_test.go` or `internal/core/storage/chaos_test.go`.
2. Helper Function: `runCrashHelperProcess()` triggered when `GO_WANT_HELPER_PROCESS == "1"`.
3. Parent Process: Spawns helper child via `exec.Command(os.Args[0], "-test.run=TestChaosCrashRecovery")`, waits for initial setup, kills child via `cmd.Process.Kill()`, then opens `dataDir`, invokes `RecoverFromWAL()`, and asserts data durability and atomicity.

---

## 5. Verification Method

To independently verify the crash recovery test once implemented:

```bash
# Run the chaos test suite specifically targeting TestChaosCrashRecovery
go test -tags=chaos -v -run TestChaosCrashRecovery ./cmd/vaultdb-server/...

# Alternatively run across all modules
go test -tags=chaos -v -run TestChaosCrashRecovery ./...
```

**Invalidation Conditions**:
- If `RecoverFromWAL()` returns an error upon reopen.
- If any committed transaction ID is missing from the recovered database.
- If any uncommitted transaction ID is found in the recovered database.
