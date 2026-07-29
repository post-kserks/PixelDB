# BRIEFING — 2026-07-29T17:15:00Z

## Mission
Implement Milestone 2: Crash Recovery Chaos Test (`TestChaosCrashRecovery` in `cmd/vaultdb-server/chaos_test.go` under `//go:build chaos`).

## 🔒 My Identity
- Archetype: implementer/qa
- Roles: implementer, qa, specialist
- Working directory: /Users/xserx/projects/pro-labs/server/.agents/worker_m2
- Original parent: 66c0470a-5ce6-4383-b96e-601734216493
- Milestone: Milestone 2 (Crash Recovery R2)

## 🔒 Key Constraints
- Build tag: `//go:build chaos`
- Helper process pattern: `GO_WANT_HELPER_PROCESS=1` via `os.Args[0]`
- Execute committed transactions (writing committed IDs to fsync'd log file) alongside active in-flight uncommitted transactions
- Forcefully kill child process using `cmd.Process.Kill()`
- Reopen DB using `storage.NewPageStorageEngine` and call `RecoverFromWAL()`
- Verify durability of committed, atomicity/undo of uncommitted, and clean recovery (no panics or catalog corruption)
- Run tests: `go test -tags=chaos -v -run TestChaosCrashRecovery ./...`
- NO CHEATING / DO NOT HARDCODE

## Current Parent
- Conversation ID: 66c0470a-5ce6-4383-b96e-601734216493
- Updated: 2026-07-29T17:15:00Z

## Task Summary
- **What to build**: Chaos crash recovery test in `cmd/vaultdb-server/chaos_test.go`
- **Success criteria**: Test passes under `go test -tags=chaos -v -run TestChaosCrashRecovery ./...`
- **Interface contracts**: storage package WAL & PageStorageEngine methods
- **Code layout**: `cmd/vaultdb-server/`

## Key Decisions Made
- Initial setup

## Change Tracker
- **Files modified**: None yet
- **Build status**: Pending
- **Pending issues**: None

## Quality Status
- **Build/test result**: Pending
- **Lint status**: Pending
- **Tests added/modified**: Pending

## Loaded Skills
- None

## Artifact Index
- ORIGINAL_REQUEST.md — Initial task instructions
