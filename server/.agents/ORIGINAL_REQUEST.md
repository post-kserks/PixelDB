# Original User Request

## Initial Request — 2026-07-29T14:09:26Z

Implement a comprehensive chaos testing suite for VaultDB to verify its reliability, ACID compliance, and recovery mechanisms. The suite must test resilience against I/O errors, unexpected process terminations, and extreme concurrency.

Working directory: /Users/xserx/projects/pro-labs/server

Integrity mode: development

## Requirements

### R1. Fault Injection (I/O Errors)
The write-ahead log (WAL) and storage engine must gracefully handle simulated disk errors (e.g., write failures, sync failures) without corrupting existing data or causing unrecoverable panics.

### R2. Crash Recovery (Abrupt Termination)
The database must not lose committed transactions or suffer data corruption if the database process is unexpectedly killed (`kill -9`).

### R3. Extreme Concurrency
The database engine must handle high concurrent access (mixed reads, writes, deletes, and background tasks like vacuuming) without deadlocks, data races, or panics.

## Acceptance Criteria

### I/O Error Resilience
- [ ] `TestChaosFaultInjection` (in `internal/core/wal/chaos_test.go`) is fully implemented and passes.
- [ ] Simulated errors trigger clean transaction rollbacks rather than panics.
- [ ] Database successfully recovers from the WAL after simulated faults.

### Crash Recovery
- [ ] A new test `TestChaosCrashRecovery` is implemented to spawn a child database process.
- [ ] The parent process forcefully kills the child process during active transactions.
- [ ] The database successfully recovers upon restart, ensuring all transactions reported as committed before the crash are intact.

### Concurrency
- [ ] A new test `TestChaosHighConcurrency` is implemented that spawns 500+ goroutines performing concurrent `SELECT`, `UPDATE`, `DELETE`, and `VACUUM` operations.
- [ ] The concurrency test passes when run with the Go race detector (`go test -race`).
- [ ] All chaos tests run seamlessly via `go test -tags=chaos ./...`.
