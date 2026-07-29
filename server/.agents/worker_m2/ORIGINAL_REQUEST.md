## 2026-07-29T14:15:04Z
<USER_REQUEST>
You are Worker 2 assigned to implement Milestone 2: Crash Recovery for Requirement R2.
Your working directory is /Users/xserx/projects/pro-labs/server/.agents/worker_m2.
The project root directory is /Users/xserx/projects/pro-labs/server.

Task Instructions:
1. Read /Users/xserx/projects/pro-labs/server/.agents/explorer_m0_2/analysis.md and /Users/xserx/projects/pro-labs/server/.agents/explorer_m0_2/handoff.md.
2. Implement `TestChaosCrashRecovery` in `cmd/vaultdb-server/chaos_test.go` (or appropriate package location) with build tag `//go:build chaos`.
3. Use the helper process pattern (`GO_WANT_HELPER_PROCESS=1` via `os.Args[0]`) to spawn a child database process.
4. The child helper process executes committed transactions (writing committed record IDs to an `fsync`-ed log file `f.Sync()`) alongside active in-flight uncommitted transactions.
5. Parent process forcefully kills child process during active execution using `cmd.Process.Kill()` (`kill -9` / `syscall.SIGKILL`).
6. Parent process reopens the database data directory using `storage.NewPageStorageEngine` and calls `RecoverFromWAL()`.
7. Parent process verifies:
   - All committed transaction records exist (Durability).
   - No uncommitted transaction records exist (Atomicity/Undo).
   - Database recovers without panics or corrupt catalog errors.
8. Run build and tests: `go test -tags=chaos -v -run TestChaosCrashRecovery ./...`.
9. Write your handoff report to `/Users/xserx/projects/pro-labs/server/.agents/worker_m2/handoff.md` with build and test command outputs.
10. Send a summary message back to the orchestrator.

MANDATORY INTEGRITY WARNING:
DO NOT CHEAT. All implementations must be genuine. DO NOT hardcode test results, create dummy/facade implementations, or circumvent the intended task. A Forensic Auditor will independently verify your work. Integrity violations WILL be detected and your work WILL be rejected.
</USER_REQUEST>
