# agentd

`agentd` is a local-first state machine daemon for AI agents. It runs out of process, stores task state in SQLite, and exposes a Unix Domain Socket (UDS) JSON Lines API so agent runtimes can coordinate long-running DAG work without becoming the source of truth.

## Features

- Durable SQLite state under `~/.agentd`
- Strict DAG node states: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`
- Lease-based node acquisition with heartbeat and timeout rollback
- Cross-provider resource locks for repository files and other shared resources
- Append-only event journal
- Provider-neutral conflict resolution with lock metadata, event history, and patch bundles
- Runtime interface discovery: `DescribeInterface`
- Health and metrics endpoints: `Health`, `Metrics`
- Versioned schema migrations
- Real DeepSeek multi-agent loop example

## Install / Run

Development:

```bash
cargo run
```

Built binary:

```bash
cargo build --release
./target/release/agentd
```

Downloaded release binary:

```bash
./agentd
```

Cargo is not required to run a built or downloaded binary.

## Default Paths

| Item | Default |
| --- | --- |
| Env file | `~/.agentd/.env` |
| SQLite DB | `~/.agentd/agent_state.db` |
| UDS socket | `~/.agentd/agentd.sock` |

Process environment variables override values from the env file.

## Configuration

Create config:

```bash
mkdir -p ~/.agentd
cp .env.example ~/.agentd/.env
chmod 600 ~/.agentd/.env
```

Important variables:

| Variable | Default | Purpose |
| --- | --- | --- |
| `AGENTD_ENV_FILE` | `~/.agentd/.env` | Env file path |
| `AGENTD_HOME` | `~/.agentd` | Base state directory |
| `AGENTD_DATABASE_URL` | `sqlite://~/.agentd/agent_state.db` | SQLite URL |
| `AGENTD_SOCKET_PATH` | `~/.agentd/agentd.sock` | UDS path |
| `AGENTD_NODE_TIMEOUT_SECS` | `300` | Lease timeout |
| `AGENTD_CONTEXT_EVENT_LIMIT` | `50` | Recent events included in acquired-node context |
| `RUST_LOG` | `agentd=info` | Logging filter |

DeepSeek loop variables:

| Variable | Default |
| --- | --- |
| `DEEPSEEK_API_KEY` | required |
| `DEEPSEEK_BASE_URL` | `https://api.deepseek.com` |
| `DEEPSEEK_MODEL` | `deepseek-v4-flash` |
| `DEEPSEEK_MAX_TOKENS` | `120` |
| `DEEPSEEK_TEMPERATURE` | `0.2` |
| `AGENTD_AGENT_WORKERS` | `2` |
| `AGENTD_ARTIFACT_DIR` | `~/.agentd/artifacts` |

## IPC Protocol

Transport: Unix Domain Socket

Framing: one JSON request per line, one JSON response per line

Request:

```json
{"id":1,"method":"Health","params":{}}
```

Response:

```json
{"id":1,"result":{"status":"ok","database":"ok","running_timeout_secs":300,"context_event_limit":50}}
```

Supported methods:

| Method | Purpose |
| --- | --- |
| `DescribeInterface` | Return protocol and method schema |
| `Health` | Check daemon and SQLite readiness |
| `Metrics` | Return runtime counters and DB gauges |
| `RegisterTask` | Create a task and DAG nodes |
| `AcquireNextNode` | Lease the next runnable node |
| `CommitEvent` | Append a journal event |
| `HeartbeatNode` | Extend a node lease |
| `CompleteNode` | Complete a leased node |
| `FailNode` | Fail a leased node |
| `TaskStatus` | Return task and node state |
| `AcquireResourceLock` | Lease a named resource such as `file:src/lib.rs` |
| `HeartbeatResourceLock` | Extend a resource lock lease |
| `ReleaseResourceLock` | Release a resource lock lease |
| `ListResourceLocks` | Return active resource locks |

Agents should call `DescribeInterface` when they do not have a generated or built-in client.

Query it from a shell:

```bash
printf '%s\n' '{"id":1,"method":"DescribeInterface","params":{}}' | nc -U ~/.agentd/agentd.sock | python3 -m json.tool
```

## Node Leases

`AcquireNextNode` returns:

- `node_id`
- `lease_id`
- `lease_owner`
- `lease_expires_at`
- synthesised execution context

Workers must pass the current `lease_id` to `CommitEvent`, `HeartbeatNode`, `CompleteNode`, and `FailNode`. If a worker stalls past `AGENTD_NODE_TIMEOUT_SECS`, the daemon rolls the node back to `PENDING`; a later worker gets a new lease, and stale lease writes are rejected.

## Cross-provider Repository Locks

Agents from different providers can collaborate in one repository by using `agentd` as the local coordination point. Before editing a file or other shared resource, acquire an exclusive resource lock with a stable `resource_key`.

Recommended key formats:

| Resource | Key example |
| --- | --- |
| Single file | `file:src/lib.rs` |
| Directory-wide operation | `dir:src` |
| Generated artifact | `artifact:docs/api.json` |
| External tool | `tool:cargo-fmt` |

Acquire a file lock:

```bash
printf '%s\n' '{
  "id": 1,
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

If the resource is already held, the result is `null`. If the lock is granted, the result includes a `lease_id`. The agent must pass that `lease_id` to `HeartbeatResourceLock` while working and to `ReleaseResourceLock` when finished.

Release a file lock:

```bash
printf '%s\n' '{
  "id": 2,
  "method": "ReleaseResourceLock",
  "params": {
    "resource_key": "file:src/lib.rs",
    "lease_id": "current-lease-uuid"
  }
}' | nc -U ~/.agentd/agentd.sock | python3 -m json.tool
```

This is provider-agnostic: Codex, Claude Code, DeepSeek scripts, local shell agents, or custom clients all use the same Unix socket and SQLite-backed lease table. Expired locks are recoverable; a later agent can acquire the same `resource_key` after the previous lease times out.

## Multi-provider Conflict Resolution

Use `agentd` as the local coordination layer when multiple providers can edit the same repository, for example two Codex workers and an Antigravity-style agent working on `docs/index.html`.

The default policy is:

- Prevent high-risk overlap with narrow `AcquireResourceLock` leases.
- Put provider-neutral patch metadata in lock `metadata`: base revision, touched paths, diff summary, assumptions, confidence, and planned verification.
- When work runs inside an agentd task, append `CommitEvent` entries for `intent`, `progress`, `result`, and `conflict`.
- If providers overlap, compare base revision, touched paths, intent, confidence, and verification output.
- Merge compatible edits, reject stale or unverifiable edits, and record the accepted decision.
- Do not use last-writer-wins as the merge policy.

Real scenario: Codex starts a docs edit and Antigravity attempts the same file.

Codex acquires the file lock with patch-bundle metadata:

```bash
printf '%s\n' '{
  "id": 10,
  "method": "AcquireResourceLock",
  "params": {
    "resource_key": "file:docs/index.html",
    "owner": "codex-docs-agent-a",
    "provider": "openai",
    "ttl_secs": 600,
    "metadata": {
      "intent": "add multi-provider conflict resolution docs",
      "base_revision": "git:HEAD",
      "touched_paths": ["docs/index.html", "docs/styles.css", "README.md"],
      "diff_summary": "document provider-neutral conflict policy and add animated flow",
      "assumptions": ["static HTML site", "CSS-only animation"],
      "confidence": "medium",
      "verification": ["python3 HTML parser sanity check"]
    }
  }
}' | nc -U ~/.agentd/agentd.sock | python3 -m json.tool
```

If Antigravity tries to acquire the same file while Codex holds it, it receives `null`:

```bash
printf '%s\n' '{
  "id": 11,
  "method": "AcquireResourceLock",
  "params": {
    "resource_key": "file:docs/index.html",
    "owner": "antigravity-docs-agent",
    "provider": "antigravity",
    "ttl_secs": 600,
    "metadata": {
      "intent": "rewrite docs conflict section",
      "base_revision": "git:HEAD",
      "touched_paths": ["docs/index.html"],
      "diff_summary": "replace conflict section with provider workflow",
      "confidence": "medium"
    }
  }
}' | nc -U ~/.agentd/agentd.sock | python3 -m json.tool
```

The coordinator should then choose one of these paths:

| Situation | Coordinator action |
| --- | --- |
| Same file, incompatible intent | Keep the first lock, ask the second provider to wait or narrow scope |
| Same file, compatible intent | Let the second provider produce a patch bundle without writing, then merge manually |
| Stale base revision | Ask the stale provider to rebase before integration |
| Failed verification | Reject that provider result and record the reason |

When the work is part of a registered DAG node, record the provider trail with `CommitEvent`:

```bash
printf '%s\n' '{
  "id": 12,
  "method": "CommitEvent",
  "params": {
    "task_id": "current-task-uuid",
    "node_id": "current-node-uuid",
    "lease_id": "current-node-lease-uuid",
    "action_type": "conflict",
    "payload": {
      "resource_key": "file:docs/index.html",
      "providers": ["openai", "antigravity"],
      "resolution": "merged Codex lock-first metadata with Antigravity patch-bundle proposal",
      "accepted_paths": ["docs/index.html", "docs/styles.css", "README.md"],
      "rejected_alternatives": ["last-writer-wins"],
      "verification": "HTML parser sanity check passed"
    }
  }
}' | nc -U ~/.agentd/agentd.sock | python3 -m json.tool
```

After verification, release the file lock:

```bash
printf '%s\n' '{
  "id": 13,
  "method": "ReleaseResourceLock",
  "params": {
    "resource_key": "file:docs/index.html",
    "lease_id": "current-resource-lock-lease-uuid"
  }
}' | nc -U ~/.agentd/agentd.sock | python3 -m json.tool
```

## Smoke Tests

Start daemon:

```bash
cargo run
```

Basic Python client:

```bash
python3 client_test.py
```

Real DeepSeek multi-agent loop:

```bash
python3 scripts/deepseek_agent_loop.py
```

The DeepSeek loop registers a four-node DAG: one agent writes a compact Python worker script, one concurrently writes the IPC contract, one waits for both and reviews integration safety, and one waits last to synthesise the result. Generated artifacts are written under `~/.agentd/artifacts` by default.
