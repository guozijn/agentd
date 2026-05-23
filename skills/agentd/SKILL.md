---
name: agentd
description: Use when working with agentd, a local-first state machine daemon for AI agents. Covers task DAG registration, node leases and heartbeats, event journals, resource locks, health and metrics, runtime interface discovery, and provider-neutral conflict resolution for Codex, Claude, Antigravity, DeepSeek, shell workers, or custom clients.
---

# agentd

Use this skill when building, operating, documenting, or integrating with `agentd`.

`agentd` is a local-first state machine daemon for AI agents. It runs out of process, stores durable state in SQLite, and exposes a Unix Domain Socket JSON Lines API so multiple runtimes can coordinate long-running work without becoming the source of truth themselves.

## Capabilities

- **Durable state:** stores tasks, DAG nodes, results, resource locks, and event history under `~/.agentd`.
- **Task DAGs:** registers work as nodes with dependencies so agents can run independent steps concurrently and join later.
- **Node leases:** grants runnable nodes with `lease_id`, lease owner, and expiry.
- **Heartbeats:** extends active node or resource leases while a worker is alive.
- **Timeout rollback:** stale running nodes return to `PENDING`; stale lease writes are rejected.
- **Event journal:** records append-only task events for intent, progress, results, failures, and conflicts.
- **Cross-provider locks:** leases named resources such as `file:src/lib.rs`, `dir:src`, `artifact:docs/api.json`, or `tool:cargo-fmt`.
- **Runtime discovery:** `DescribeInterface` returns supported methods and payload shapes.
- **Health checks:** `Health` reports daemon and database readiness.
- **Metrics:** `Metrics` reports runtime counters and database gauges.
- **Provider-neutral coordination:** Codex, Claude, Antigravity, DeepSeek scripts, shell workers, and custom clients use the same socket protocol.
- **Conflict resolution:** overlapping provider edits are coordinated with locks, events, and patch bundles instead of last-writer-wins.

## Default Paths

| Item | Default |
| --- | --- |
| Env file | `~/.agentd/.env` |
| SQLite DB | `~/.agentd/agent_state.db` |
| UDS socket | `~/.agentd/agentd.sock` |

## IPC Basics

Transport is Unix Domain Socket JSON Lines: send one JSON request per line and read one JSON response per line.

Query the runtime contract:

```bash
printf '%s\n' '{"id":1,"method":"DescribeInterface","params":{}}' \
  | nc -U ~/.agentd/agentd.sock \
  | python3 -m json.tool
```

Core methods:

| Method | Purpose |
| --- | --- |
| `DescribeInterface` | Return protocol and method schema. |
| `Health` | Check daemon and SQLite readiness. |
| `Metrics` | Return runtime counters and DB gauges. |
| `RegisterTask` | Create a task and DAG nodes. |
| `AcquireNextNode` | Lease the next runnable node. |
| `CommitEvent` | Append a journal event. |
| `HeartbeatNode` | Extend a node lease. |
| `CompleteNode` | Complete a leased node. |
| `FailNode` | Fail a leased node. |
| `TaskStatus` | Return task and node state. |
| `AcquireResourceLock` | Lease a named resource. |
| `HeartbeatResourceLock` | Extend a resource lock lease. |
| `ReleaseResourceLock` | Release a resource lock lease. |
| `ListResourceLocks` | Return active resource locks. |

## Task DAG Workflow

1. Register a task with `RegisterTask`, including `initial_nodes`.
2. Workers call `AcquireNextNode` with a `lease_owner`.
3. A worker may append `CommitEvent` entries while running.
4. A worker must call `HeartbeatNode` before lease expiry for long work.
5. Finish with `CompleteNode` or `FailNode` using the current `lease_id`.
6. Inspect overall state with `TaskStatus` or `Metrics`.

Minimal task registration:

```bash
printf '%s\n' '{
  "id": 1,
  "method": "RegisterTask",
  "params": {
    "goal": "multi-agent docs update",
    "context": {"repo": "agentd"},
    "initial_nodes": [
      {"dependencies": [], "payload_schema": {"role": "writer"}},
      {"dependencies": [], "payload_schema": {"role": "reviewer"}}
    ]
  }
}' | nc -U ~/.agentd/agentd.sock | python3 -m json.tool
```

Acquire work:

```bash
printf '%s\n' '{
  "id": 2,
  "method": "AcquireNextNode",
  "params": {
    "task_id": "current-task-uuid",
    "lease_owner": "codex-worker-a"
  }
}' | nc -U ~/.agentd/agentd.sock | python3 -m json.tool
```

Complete work:

```bash
printf '%s\n' '{
  "id": 3,
  "method": "CompleteNode",
  "params": {
    "task_id": "current-task-uuid",
    "node_id": "current-node-uuid",
    "lease_id": "current-node-lease-uuid",
    "result_payload": {"ok": true, "summary": "docs updated"}
  }
}' | nc -U ~/.agentd/agentd.sock | python3 -m json.tool
```

## Resource Lock Workflow

Use resource locks when multiple providers may write the same file, directory, generated artifact, or external tool.

Recommended keys:

| Resource | Key example |
| --- | --- |
| Single file | `file:src/lib.rs` |
| Directory operation | `dir:src` |
| Generated artifact | `artifact:docs/api.json` |
| External tool | `tool:cargo-fmt` |

Acquire a lock:

```bash
printf '%s\n' '{
  "id": 4,
  "method": "AcquireResourceLock",
  "params": {
    "resource_key": "file:src/lib.rs",
    "owner": "codex-cli",
    "provider": "openai",
    "ttl_secs": 300,
    "metadata": {"intent": "edit"}
  }
}' | nc -U ~/.agentd/agentd.sock | python3 -m json.tool
```

If the result is `null`, another worker holds the resource. Wait, narrow scope, or ask the coordinator to resolve.

Heartbeat and release:

```bash
printf '%s\n' '{
  "id": 5,
  "method": "HeartbeatResourceLock",
  "params": {
    "resource_key": "file:src/lib.rs",
    "lease_id": "current-resource-lock-lease-uuid",
    "ttl_secs": 300
  }
}' | nc -U ~/.agentd/agentd.sock | python3 -m json.tool

printf '%s\n' '{
  "id": 6,
  "method": "ReleaseResourceLock",
  "params": {
    "resource_key": "file:src/lib.rs",
    "lease_id": "current-resource-lock-lease-uuid"
  }
}' | nc -U ~/.agentd/agentd.sock | python3 -m json.tool
```

## Multi-Provider Conflict Resolution

Use this section when multiple agents or providers can touch the same repository, files, generated artifacts, or logical work units. The goal is to avoid blind overwrites and make every merge decision auditable.

### Core Policy

Never use last-writer-wins as the default merge rule.

Use three layers:

1. **Locks prevent unsafe overlap.** Acquire narrow locks for files, directories, generated artifacts, tools, or logical work units.
2. **Events explain work.** Record `intent`, `progress`, `result`, and `conflict` events when the environment supports a journal.
3. **Patch bundles drive merges.** Compare provider outputs by metadata and verification, not by provider name or arrival order.

### Patch Bundle

Every provider result should include:

- `provider`: provider/runtime name, such as `openai`, `anthropic`, `antigravity`, or `custom`.
- `owner`: concrete agent identity.
- `base_revision`: git SHA, branch name, or other stable starting point.
- `touched_paths`: files or resources the provider read or changed.
- `diff_summary`: concise behavioral summary.
- `assumptions`: important constraints the provider relied on.
- `confidence`: low, medium, or high.
- `verification`: commands run and outcomes.
- `conflicts`: observed overlaps, stale bases, failed checks, or incompatible intent.

### Workflow

1. Inspect current state with `git status -sb` and the relevant diff.
2. Assign disjoint ownership when possible.
3. Before editing shared resources, acquire or request a narrow lock.
4. If a lock is unavailable, do not write over the owner. Wait, narrow scope, or submit a patch bundle for coordinator review.
5. Record provider intent before work begins when an event journal is available.
6. On completion, collect patch bundles from every provider.
7. Detect conflicts by comparing `base_revision`, `touched_paths`, and intent.
8. Resolve by preserving compatible edits, rejecting stale or unverifiable changes, and recording discarded alternatives.
9. Verify the merged result with the smallest meaningful checks.
10. Release locks and record the final decision.

### Conflict Decisions

| Situation | Decision |
| --- | --- |
| Same file, incompatible intent | Keep the active lock; ask later providers to wait or narrow scope. |
| Same file, compatible intent | Merge manually from patch bundles after checking base freshness. |
| Stale base revision | Ask that provider to rebase before integration. |
| Missing verification | Treat as lower confidence; verify locally before accepting. |
| Failed verification | Reject or repair before merge; record the reason. |
| Generated artifact overlap | Prefer a single owner or regenerate from source after merge. |

### agentd IPC Pattern

Use resource locks for shared files and put patch-bundle metadata in `metadata`:

```bash
printf '%s\n' '{
  "id": 1,
  "method": "AcquireResourceLock",
  "params": {
    "resource_key": "file:docs/index.html",
    "owner": "codex-docs-agent",
    "provider": "openai",
    "ttl_secs": 600,
    "metadata": {
      "intent": "update docs conflict section",
      "base_revision": "git:HEAD",
      "touched_paths": ["docs/index.html", "docs/styles.css"],
      "diff_summary": "add provider-neutral conflict workflow",
      "assumptions": ["static HTML", "CSS-only animation"],
      "confidence": "medium",
      "verification": ["python3 HTML parser sanity check"]
    }
  }
}' | nc -U ~/.agentd/agentd.sock | python3 -m json.tool
```

If the response is `null`, another provider owns the resource. Do not write that resource unless the coordinator explicitly resolves the conflict.

For DAG work, journal the resolution:

```bash
printf '%s\n' '{
  "id": 2,
  "method": "CommitEvent",
  "params": {
    "task_id": "current-task-uuid",
    "node_id": "current-node-uuid",
    "lease_id": "current-node-lease-uuid",
    "action_type": "conflict",
    "payload": {
      "resource_key": "file:docs/index.html",
      "providers": ["openai", "antigravity"],
      "resolution": "merged compatible edits from patch bundles",
      "rejected_alternatives": ["last-writer-wins"],
      "verification": "HTML parser sanity check passed"
    }
  }
}' | nc -U ~/.agentd/agentd.sock | python3 -m json.tool
```

Release the lock after the merge and verification:

```bash
printf '%s\n' '{
  "id": 3,
  "method": "ReleaseResourceLock",
  "params": {
    "resource_key": "file:docs/index.html",
    "lease_id": "current-resource-lock-lease-uuid"
  }
}' | nc -U ~/.agentd/agentd.sock | python3 -m json.tool
```

## Coordinator Output

When reporting the result, include:

- Providers involved.
- Files/resources that overlapped.
- Accepted changes.
- Rejected or superseded alternatives.
- Verification performed.
- Any locks left active or follow-up rebases needed.
