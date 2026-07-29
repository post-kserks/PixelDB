# VaultDB Crash Recovery (R2) Technical Analysis Report

## Executive Summary
This report analyzes Requirement R2 (**Crash Recovery / Abrupt Termination**) for VaultDB. The objective is to design `TestChaosCrashRecovery`, a chaos test that verifies VaultDB's ACID durability and atomicity under abrupt process termination (`kill -9` / `syscall.SIGKILL`). 

The analysis covers:
1. Architecture of VaultDB process lifecycle, server entry points (`cmd/vaultdb-server`), and storage/WAL persistence.
2. Structure of ARIES-based 3-phase crash recovery (`RecoverFromWAL`).
3. Detailed technical specification for implementing `TestChaosCrashRecovery` using the Go `os.Args[0]` helper process pattern.

---

## 1. Process Lifecycle & Server Entry Points

### 1.1 Server Entry Point (`cmd/vaultdb-server/main.go`)
- **CLI Flags**:
  - `-host` (default `127.0.0.1`)
  - `-port` (default `5432` - PostgreSQL Wire Protocol server)
  - `-http-port` (default `8080` - REST API & Web UI)
  - `-monitor-port` (default `5433` - Health/metrics endpoint)
  - `-data` (default `./data` - Data directory path)
  - `-config` (optional YAML configuration path)
- **Environment Overrides**:
  - `VAULTDB_AUTH_ENABLED` (boolean; set `"false"` during test execution to bypass token authentication).
  - `VAULTDB_LOG_LEVEL` (`debug` or `info`).

### 1.2 Data Persistence Architecture
- **Data Directory Layout (`-data <dir>`)**:
  - `wal/vaultdb.wal`: Single append-only WAL file containing operations and transaction markers (`OpPageInsert`, `OpPageDelete`, `OpCommit`, `OpAbort`, `OpCheckpoint`).
  - `pagedb/<dbname>/<tablename>.heap`: Slotted-page heap storage files storing actual page data.
  - `catalog.json`: Serialized system catalog containing table schemas and checkpoint LSN.
- **Engine Components (`internal/core/storage/page_engine.go`)**:
  - `PageStorageEngine`: Main storage engine implementing `StorageEngine` interface.
  - `WAL` (`internal/core/wal`): Handles append-only log writes with CRC32 checksums.
  - `Manager` (`internal/core/txmanager`): Allocates transaction IDs (`XID`), tracks active transactions, manages tuple visibility, and performs commit/rollback operations.

### 1.3 WAL Crash Recovery Flow (`RecoverFromWAL()`)
When VaultDB starts or reopens a database directory after a crash, `PageStorageEngine.RecoverFromWAL()` executes an ARIES-style 3-phase recovery:
1. **Analysis Phase**: `wal.AnalyzeTransactions()` scans the WAL file from start to finish.
   - Identifies all `committed` transaction IDs (transactions with an `OpCommit` record).
   - Identifies all `inProgress` (uncommitted / active) transaction IDs.
2. **Redo Phase**: `redoPhase()` replays all WAL log entries sequentially (both committed and uncommitted) onto page storage, restoring heap page state up to the crash point.
3. **Undo Phase**: `undoPhase(inProgress)` iterates through all `inProgress` transactions and rolls back their modifications (marking inserted tuples as dead or restoring original state).
4. **Post-Recovery Finalization**:
   - `Sync()` is called on all open heap files.
   - Catalog is recalculated and saved to `catalog.json`.
   - A new WAL `OpCheckpoint` record is written, and WAL is truncated.

---

## 2. Design Specification for `TestChaosCrashRecovery`

### 2.1 Overview & Requirements
- **Goal**: Verify that when a VaultDB server process is forcefully terminated via `kill -9` (`syscall.SIGKILL`) during active transaction execution:
  1. Transactions that received confirmed `COMMIT` responses before the crash are **100% durable** and intact upon restart.
  2. Transactions that were active/uncommitted (or in-flight without `COMMIT` ack) at crash time are **cleanly rolled back** without leaving orphan or dirty data.
  3. Reopening the database triggers `RecoverFromWAL()` cleanly without panics, data corruption, or log recovery errors.
- **Build Tag**: `//go:build chaos`
- **Location**: `cmd/vaultdb-server/chaos_test.go` or `internal/core/storage/chaos_test.go`.

### 2.2 Child Process Spawning Pattern
Go standard library tests handle child process spawning via the `os.Args[0]` helper process trick:

```go
//go:build chaos

package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func TestChaosCrashRecovery(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") == "1" {
		runCrashHelperProcess()
		return
	}
	// Parent process orchestration logic...
}
```

### 2.3 Step-by-Step Implementation Architecture

#### Step 1: Parent Process Setup
1. Create temporary test directory: `dataDir := t.TempDir()`.
2. Create communication log file: `commPath := filepath.Join(dataDir, "committed_ids.txt")`.
3. Spawn child helper process using `exec.Command(os.Args[0], "-test.run=TestChaosCrashRecovery")`.
4. Pass required environment variables:
   - `GO_WANT_HELPER_PROCESS=1`
   - `CHAOS_DATA_DIR=dataDir`
   - `CHAOS_COMM_PATH=commPath`
   - `VAULTDB_AUTH_ENABLED=false`

#### Step 2: Child Helper Process Execution (`runCrashHelperProcess`)
1. Read `CHAOS_DATA_DIR` and `CHAOS_COMM_PATH` from environment.
2. Initialize `WAL`, `txmanager.Manager`, and `storage.NewPageStorageEngine(dataDir, wal, txm)`.
3. Create test database (`crash_db`) and table (`users (id INT, val TEXT)`).
4. Run high-throughput transaction loop:
   - **Committed Worker**:
     - Begins transaction, inserts row `(id, val)`, commits transaction via `txm` / `executor`.
     - Upon successful commit, appends `id` to `CHAOS_COMM_PATH` and invokes `file.Sync()` to ensure the committed log record is flushed to disk prior to process termination.
   - **Uncommitted Worker**:
     - Begins transaction, inserts row `(999990 + i, "uncommitted")`.
     - Holds transaction active in memory without calling `Commit()`.
5. Continuously repeat operations until killed by parent.

#### Step 3: Forceful Process Termination (`kill -9`)
1. Parent process lets child run for a controlled window (e.g., 300ms - 1000ms) to generate significant committed and active uncommitted state.
2. Parent issues forceful kill:
   ```go
   if err := cmd.Process.Kill(); err != nil { // Sends SIGKILL (kill -9)
       t.Fatalf("failed to kill child process: %v", err)
   }
   _ = cmd.Wait() // Wait for process termination
   ```

#### Step 4: Post-Crash Recovery & Verification
1. Read `committed_ids.txt` to collect the set of all transaction IDs acknowledged as committed prior to `SIGKILL`.
2. Open the crashed database directory directly:
   ```go
   w, err := wal.Open(filepath.Join(dataDir, "wal", "vaultdb.wal"))
   txm := txmanager.NewManager()
   engine, err := storage.NewPageStorageEngine(dataDir, w, txm)
   ```
3. Run ARIES WAL recovery:
   ```go
   if err := engine.RecoverFromWAL(); err != nil {
       t.Fatalf("WAL recovery failed after SIGKILL: %v", err)
   }
   ```
4. Perform Data Verification:
   - **Durability Verification**: Count rows in `users`. Query all rows and assert that EVERY `id` recorded in `committed_ids.txt` exists in the recovered table.
   - **Atomicity / Undo Verification**: Assert that NO `uncommitted` row IDs (>= 999990) are present in the recovered table.
   - **Catalog & Storage Integrity**: Ensure catalog table schemas match, and subsequent `InsertRows` / `CountRows` operate normally without errors.

---

## 3. Comparison of Alternatives

| Approach | Pros | Cons | Decision |
|---|---|---|---|
| **`os.Args[0]` Helper Process (Selected)** | No external `go build` required, portable across OS, instant execution within `go test` runner. | Requires handling `GO_WANT_HELPER_PROCESS` flag inside test file. | **RECOMMENDED** |
| **`go build cmd/vaultdb-server` External Binary** | Tests exact production binary main.go entry point. | Requires Go compiler in runtime PATH, slow compile overhead in test runs. | Secondary / Fallback |
| **In-process Goroutine Panic Simulation** | Simple setup. | Does NOT test real OS process termination, leaves open file descriptors and shared memory intact. | Unsuitable for R2 |

---

## 4. Recommendations for Implementer
1. Place the test file at `cmd/vaultdb-server/chaos_test.go` or `internal/core/storage/chaos_recovery_test.go`.
2. Annotate the file header with `//go:build chaos`.
3. Use synchronized file logging (`fsync`) for tracking committed IDs in the child helper process so test assertion reflects true pre-kill commit responses.
4. Ensure `t.Cleanup` handles temporary file cleanup.
