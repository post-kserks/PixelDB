# Project: VaultDB Chaos Testing Suite

## Architecture
- **Storage & WAL Engine**: `internal/core/wal`, `internal/core/storage`, `internal/core/txmanager`, `internal/core/executor`.
- **Chaos Tests**: Gated by `//go:build chaos` across modules.
- **I/O Error Injection**: Interface abstraction `File` in `internal/core/wal/wal.go` allowing `FaultyFile` injection in `chaos_test.go`.
- **Crash Recovery**: `os.Args[0]` child helper process executing transactions killed by parent via `kill -9` (`syscall.SIGKILL`), verified with `RecoverFromWAL()`.
- **Extreme Concurrency**: 500+ goroutines (520 total: 200 Readers, 150 Updaters, 75 Inserters, 70 Deleters, 25 Vacuumers) exercising `Session`, `PageStorageEngine`, `Vacuum`, and `txmanager`.

## Milestones
| # | Name | Scope | Dependencies | Status |
|---|------|-------|-------------|--------|
| 0 | Exploration | Codebase inspection, existing tests & WAL structure | None | DONE |
| 1 | Fault Injection | `internal/core/wal/chaos_test.go` + `File` interface + page unpin fix | M0 | DONE |
| 2 | Crash Recovery | `TestChaosCrashRecovery` child process + ARIES recovery verification | M0 | DONE |
| 3 | Extreme Concurrency | `TestChaosHighConcurrency` 500+ goroutines + build tags | M0 | DONE |
| 4 | Final Verification & Audit Gate | Reviewers, Challengers, and Forensic Auditor verification | M1, M2, M3 | DONE |

## Interface Contracts
- Build tag convention: `//go:build chaos` at line 1 of all chaos test files.
- WAL File interface: `io.Reader`, `io.Writer`, `io.Closer`, `io.Seeker`, `Sync() error`, `Truncate(int64) error`, `Stat() (os.FileInfo, error)`.
- Crash Recovery contract: Child process `kill -9` during active transactions must restore committed rows and undo uncommitted rows upon `RecoverFromWAL()`.
- Error handling contract: Fault injection must return errors to callers without panicking or corrupting WAL headers or leaking page pins in buffer pool.
