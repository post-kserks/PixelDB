# VaultDB WAL & Storage Engine Fault Injection Analysis Report (Requirement R1)

## Executive Summary
This analysis evaluates Requirement R1 (Fault Injection for WAL & Storage Engine) in VaultDB. The objective is to analyze how simulated I/O errors (write/sync failures) can be injected during runtime testing, evaluate error handling and transaction rollback behavior, verify recovery capabilities, and identify any panic risks or resource leaks.

---

## 1. Existing Chaos Test Analysis (`internal/core/wal/chaos_test.go`)

### 1.1 Current Skeleton & Build Status
- **File**: `internal/core/wal/chaos_test.go`
- **Build Tag**: `//go:build chaos` (Line 1)
- **Current Compiler Error**:
  Running `go test -tags=chaos ./internal/core/wal/...` fails with:
  ```
  internal/core/wal/chaos_test.go:53:11: cannot use faultyFile (variable of type *FaultyFile) as *os.File value in assignment
  ```

### 1.2 Root Cause of Compilation Failure
- **In `internal/core/wal/wal.go`**:
  Line 492 defines `WAL` with a concrete `*os.File` field:
  ```go
  type WAL struct {
      file *os.File
      ...
  }
  ```
- **In `internal/core/wal/chaos_test.go`**:
  Lines 17-35 define `FaultyFile`:
  ```go
  type FaultyFile struct {
      *os.File
      WriteErrRate float64
      SyncErrRate  float64
  }
  func (f *FaultyFile) Write(b []byte) (int, error) { ... }
  func (f *FaultyFile) Sync() error { ... }
  ```
  Line 53 attempts `w.file = faultyFile`. Since `w.file` is declared as `*os.File`, Go's type system rejects assigning `*FaultyFile` (even though it embeds `*os.File`) because type identity differs.

---

## 2. Recommended Clean Fault Injection Mechanism

### 2.1 WAL File Interface Abstraction
To allow clean, runtime I/O error injection without modifying internal WAL logic, `WAL` in `internal/core/wal/wal.go` should depend on a `File` interface rather than `*os.File`:

```go
// File defines the filesystem interface required by WAL.
type File interface {
    io.Reader
    io.Writer
    io.Closer
    io.Seeker
    Sync() error
    Truncate(size int64) error
    Stat() (os.FileInfo, error)
}
```

### 2.2 Enabling `FaultyFile` Integration
`FaultyFile` in `chaos_test.go` already embeds `*os.File` and implements `Write(b []byte)` and `Sync()`. Because it embeds `*os.File`, `*FaultyFile` automatically implements `io.Reader`, `io.Closer`, `io.Seeker`, `Truncate`, and `Stat`. 

Once `WAL.file` is typed as `File`, `w.file = faultyFile` compiles seamlessly and allows configuring arbitrary `WriteErrRate` and `SyncErrRate` during chaos tests.

### 2.3 Storage Engine Fault Injection Strategy
- **BufferPool & HeapFile Layer**:
  - `HeapFile` (`internal/core/storage/heap/heapfile.go`) handles segment files using `mmapFile`.
  - `BufferPool` (`internal/core/storage/buffer_pool.go`) writes pages to disk via `buf.hf.WritePage(pid, page)`.
  - To test storage-level I/O failures during page eviction (`evict`) or `FlushAll()`, a wrapped file interface or mock `HeapFile` can be injected into `BufferPool`.
  - When `WritePage` fails during `evict()` or `FlushAll()`, `BufferPool` retains the page as `dirty = true` and returns the write error up the stack.

---

## 3. Transaction Rollback & Error Propagation Analysis

### 3.1 Write & Sync Error Flow
1. **WAL Append Failure**:
   - In `WAL.appendBytesLocked` (`wal.go` line 673):
     `w.file.Write(record)` or `w.file.Sync()` returns an error (e.g. `ENOSPC` or `EIO`).
   - `AppendWithTx` propagates `0, err` directly back to `PageStorageEngine`.

2. **Storage Engine Operation Interruption**:
   - In `PageStorageEngine.InsertRows` (`page_engine_io.go` lines 392, 570):
     When `e.wal.AppendWithTx` returns an error, `InsertRows` stops processing immediately and returns `0, fmt.Errorf("wal insert: %w", err)`.
   - The transaction's `OpCommit` record is **never written** to WAL.

3. **Transaction Abort & Visibility Handling**:
   - In `TxManager` (`manager.go` line 640):
     `m.Rollback(tx, catalog)` marks transaction state as `TxIdle` and clears buffered operations.
   - For in-memory data, uncommitted tuples have `xmin = txID`. When concurrent or subsequent queries call `readRows`, `xmin` is evaluated via `e.txMgr.IsCommitted(xmin)`. Since `OpCommit` was never written and `txID` was not committed, `IsCommitted` returns `false`, ensuring uncommitted tuples are invisible to all readers.

4. **Crash & WAL Recovery Durability**:
   - In `wal.Open()` (`wal.go` line 531) and `scanAndTruncate()` (`recovery.go` line 244):
     If a crash or write failure leaves a partial or corrupt record at the tail of the WAL file, `scanAndTruncate` detects the corrupt magic/checksum and truncates the file to the last valid record.
   - `RecoverFromWAL()` (`page_engine.go` line 144) only replays committed transactions. Uncommitted operations resulting from aborted/failed WAL writes are ignored.

---

## 4. Panic Risk & Bug Inspection

### 4.1 Explicit `panic()` Inspection
A search across `internal/core` identified 4 explicit `panic()` calls:
1. `internal/core/storage/lwlock.go`:
   - Line 86: `panic("RUnlock on unlocked or write-locked LWRLock")`
   - Line 107: `panic("Unlock on unlocked or read-locked LWRLock")`
2. `internal/core/wal/group_commit.go`:
   - Line 19: `panic("wal: EnableGroupCommit called twice")`
   - Line 41: `panic("wal: EnableWriteBehind called twice")`

No explicit `panic()` calls exist in WAL write/sync error paths or recovery paths.

### 4.2 Resource & Pin Leak Discovery on WAL Write Failure
In `PageStorageEngine.InsertRows` (`internal/core/storage/page_engine_io.go`, lines 569-574):
```go
lsn, err = e.wal.AppendWithTx(txID, wal.OpPageInsert, payload)
if err != nil {
    e.pageLock.UnlockPageWrite(pid)
    return 0, fmt.Errorf("wal insert: %w", err)
}
```
**Observation**:
Before calling `AppendWithTx`, `getPage` pins the page (`pinCnt > 0`). If `AppendWithTx` returns an error, `UnlockPageWrite` is called, but `e.bufPool.UnpinPage(pid, false)` is **omitted**.

**Impact**:
If `AppendWithTx` fails, the page remains pinned in `BufferPool`. Under continuous simulated fault injection, pages could remain pinned indefinitely, preventing clock-sweep eviction and leading to buffer pool exhaustion (`"evict: all pages pinned"`).

**Mitigation Requirement for Implementer**:
Ensure `e.bufPool.UnpinPage(pid, false)` is called on the WAL error branch before returning.

---

## 5. Verification Plan for Requirement R1

1. **Compilation & Tag Verification**:
   - Update `WAL.file` field in `internal/core/wal/wal.go` from `*os.File` to `File` interface.
   - Execute:
     ```bash
     go test -tags=chaos -v ./internal/core/wal -run TestChaosFaultInjection
     ```
2. **Crash Recovery Verification**:
   - Execute:
     ```bash
     go test -tags=chaos -v ./internal/core/wal -run TestChaosCrashRecovery
     ```
3. **Full Suite Verification**:
   - Execute:
     ```bash
     go test -tags=chaos ./...
     ```
