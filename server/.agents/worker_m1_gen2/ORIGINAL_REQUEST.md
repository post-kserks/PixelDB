## 2026-07-29T17:47:38Z
You are Worker 1 (Gen2) assigned to implement Milestone 1: Fault Injection (I/O Errors) for Requirement R1.
Your working directory is /Users/xserx/projects/pro-labs/server/.agents/worker_m1_gen2.
The project root directory is /Users/xserx/projects/pro-labs/server.

Task Instructions:
1. Read /Users/xserx/projects/pro-labs/server/.agents/explorer_m0_1/analysis.md and /Users/xserx/projects/pro-labs/server/.agents/explorer_m0_1/handoff.md.
2. In `internal/core/wal/wal.go`: Define a `File` interface containing `io.Reader`, `io.Writer`, `io.Closer`, `io.Seeker`, `Sync() error`, `Truncate(int64) error`, `Stat() (os.FileInfo, error)`. Replace `file *os.File` in `WAL` struct with `file File`.
3. In `internal/core/storage/page_engine_io.go`: In `InsertRows` (around lines 569-574), ensure `e.bufPool.UnpinPage(pid, false)` is called when `e.wal.AppendWithTx` returns an error to avoid page pin leaks on WAL write failure.
4. Ensure `internal/core/wal/chaos_test.go` compiles and passes `TestChaosFaultInjection`. Verify simulated errors trigger clean transaction rollbacks rather than panics, and the database recovers successfully from the WAL.
5. Run build and tests: `go test -tags=chaos -v ./internal/core/wal/...` and `go test -tags=chaos ./internal/core/storage/...`.
6. Write your handoff report to `/Users/xserx/projects/pro-labs/server/.agents/worker_m1_gen2/handoff.md` with build and test command outputs.
7. Update `/Users/xserx/projects/pro-labs/server/.agents/worker_m1_gen2/progress.md`.
8. Send a summary message back to the orchestrator.
