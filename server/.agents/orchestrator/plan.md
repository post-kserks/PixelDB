# Plan — VaultDB Chaos Testing Suite Implementation

## Objective
Implement a robust, production-grade chaos testing suite for VaultDB covering I/O fault injection, crash recovery, and extreme concurrency, verified with Go race detector and chaos build tags.

## Strategy
Follow the Project Pattern:
1. **Exploration**: Spawn Explorer subagents (`teamwork_preview_explorer`) to inspect existing codebase, WAL implementation, storage engine, transaction management, process CLI capabilities, and existing test setup.
2. **Decomposition**:
   - Milestone 1 (M1): Fault Injection (I/O Errors) in `internal/core/wal/chaos_test.go` and associated error handling in WAL/storage engine.
   - Milestone 2 (M2): Crash Recovery test `TestChaosCrashRecovery` (child process execution, simulated crashes via `kill -9`, state verification).
   - Milestone 3 (M3): Extreme Concurrency test `TestChaosHighConcurrency` (500+ goroutines, mixed workload: SELECT/UPDATE/DELETE/VACUUM, race detector compliance).
3. **Execution per Milestone**:
   - Explorer analyzes requirements & files -> Worker implements solution & runs tests -> Reviewers (2) review code quality & correctness -> Challengers (2) perform stress & edge case verification -> Forensic Auditor checks for hardcoding/cheating.
4. **Final Verification**:
   - Execute full test suite with `go test -tags=chaos ./...` and `go test -race -tags=chaos ./...`.
5. **Completion**: Send victory message to Sentinel.
