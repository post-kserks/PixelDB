## 2026-07-29T17:15:04Z

<USER_REQUEST>
You are Worker 3 assigned to implement Milestone 3: Extreme Concurrency for Requirement R3.
Your working directory is /Users/xserx/projects/pro-labs/server/.agents/worker_m3.
The project root directory is /Users/xserx/projects/pro-labs/server.

Task Instructions:
1. Read /Users/xserx/projects/pro-labs/server/.agents/explorer_m0_3/analysis.md and /Users/xserx/projects/pro-labs/server/.agents/explorer_m0_3/handoff.md.
2. Ensure `//go:build chaos` tag is present on line 1 of `internal/core/executor/chaos_test.go` and `internal/core/txmanager/chaos_test.go`.
3. Implement `TestChaosHighConcurrency` in `internal/core/executor/chaos_test.go` with build tag `//go:build chaos`.
4. Test topology: Spawn 500+ goroutines (e.g. 520 total: 200 Readers, 150 Updaters, 75 Inserters, 70 Deleters, 25 Vacuumers) performing concurrent operations (`SELECT`, `UPDATE`, `DELETE`, `VACUUM`).
5. Each worker goroutine instantiates its own dedicated `Session` (`executor.NewSession(...)`) while sharing underlying `PageStorageEngine` and `txmanager.Manager`.
6. Handle expected OCC transaction conflict errors (`"conflict"`, `"transaction conflict"`, `"duplicate key"`) gracefully. Assert zero panics, zero data races, and 100% data integrity post-test.
7. Run build and tests: `go test -race -tags=chaos -v -run TestChaosHighConcurrency ./internal/core/executor/...`.
8. Write your handoff report to `/Users/xserx/projects/pro-labs/server/.agents/worker_m3/handoff.md` with build and test command outputs.
9. Send a summary message back to the orchestrator.

MANDATORY INTEGRITY WARNING:
DO NOT CHEAT. All implementations must be genuine. DO NOT hardcode test results, create dummy/facade implementations, or circumvent the intended task. A Forensic Auditor will independently verify your work. Integrity violations WILL be detected and your work WILL be rejected.
</USER_REQUEST>
