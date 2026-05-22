# agentd

`agentd` is a local-first, out-of-process state machine daemon for AI agents. It owns durable task state, DAG node transitions, and an append-only event journal so Python, Node, or other agent runtimes can crash and resume without becoming the source of truth.

The daemon is model-agnostic. Agents communicate over a Unix Domain Socket using newline-delimited JSON-RPC style messages.

## Current MVP

- Rust 2021 daemon built on `tokio`
- Unix Domain Socket IPC with JSON Lines framing
- Embedded SQLite via `sqlx`
- Durable task, DAG node, and event journal tables
- Versioned schema migrations with legacy MVP database adoption
- Strict node states: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`
- Dependency-aware node acquisition
- Lease IDs, heartbeats, and timeout rollback for stale `RUNNING` nodes
- Bounded context journals to keep agent prompts small
- Runtime IPC discovery through `DescribeInterface`
- Health checks through `Health`
- Structured runtime and database metrics through `Metrics`
- Python smoke clients, including a real DeepSeek-backed multi-agent loop

## Storage

By default, persistent state is stored under:

```text
~/.agentd/agent_state.db
```

This keeps runtime state out of the repository while preserving local-first operation. Override it with:

```bash
AGENTD_DATABASE_URL=sqlite:///absolute/path/to/agent_state.db cargo run
```

You can also set `AGENTD_HOME` to change the default state directory:

```bash
AGENTD_HOME=/var/lib/agentd cargo run
```

## Configuration

| Variable | Default | Purpose |
| --- | --- | --- |
| `AGENTD_DATABASE_URL` | `sqlite://$HOME/.agentd/agent_state.db` | SQLite database URL |
| `AGENTD_HOME` | `$HOME/.agentd` | Base directory used when `AGENTD_DATABASE_URL` is unset |
| `AGENTD_SOCKET_PATH` | `/tmp/agentd.sock` | Unix socket path |
| `AGENTD_NODE_TIMEOUT_SECS` | `300` | Stale `RUNNING` node rollback threshold |
| `AGENTD_CONTEXT_EVENT_LIMIT` | `50` | Maximum recent journal events included in acquired node context |
| `RUST_LOG` | `agentd=info` | Logging filter |

## Run The Daemon

```bash
cargo run
```

The daemon removes stale socket files on startup and sets the socket permissions to `0660`. Authentication is intentionally left to Unix file permissions for the MVP.

## IPC Methods

Requests are one JSON object per line:

```json
{"id":1,"method":"RegisterTask","params":{"goal":"demo","context":{},"initial_nodes":[]}}
```

Responses are also one JSON object per line:

```json
{"id":1,"result":{"task_id":"..."}}
```

Supported methods:

| Method | Purpose |
| --- | --- |
| `DescribeInterface` | Return the runtime IPC contract so agents can discover supported methods |
| `Health` | Check daemon and SQLite reachability |
| `Metrics` | Return low-overhead runtime counters and database gauges |
| `RegisterTask` | Create a task and initial DAG nodes |
| `AcquireNextNode` | Acquire the next runnable `PENDING` node, mark it `RUNNING`, and return a `lease_id` |
| `CommitEvent` | Append an event to the durable journal; include `lease_id` while the node is `RUNNING` |
| `HeartbeatNode` | Extend a `RUNNING` node lease with the current `lease_id` |
| `CompleteNode` | Mark a `RUNNING` node `COMPLETED` with the current `lease_id` and persist its result |
| `FailNode` | Mark a `RUNNING` node `FAILED` with the current `lease_id` and persist the error |
| `TaskStatus` | Return task metadata, status counts, and nodes |

Agents should call `DescribeInterface` first when they are not using a prebuilt client. This lets an autonomous worker discover the protocol, node state model, method names, parameter shapes, and result shapes directly from the daemon instead of relying on copied documentation.

When an agent acquires a node, the response includes `lease_id`, `lease_owner`, and `lease_expires_at`. Worker-side `CommitEvent`, `HeartbeatNode`, `CompleteNode`, and `FailNode` calls must send the current `lease_id`. If a worker stalls past `AGENTD_NODE_TIMEOUT_SECS`, the daemon rolls the node back to `PENDING`; a later worker receives a new lease, and stale calls using the old lease are rejected.

## Python Smoke Test

In one terminal:

```bash
cargo run
```

In another:

```bash
python3 client_test.py
```

## DeepSeek Multi-Agent Loop

Create the agentd config directory and copy the example env file there:

```bash
mkdir -p ~/.agentd
cp .env.example ~/.agentd/.env
chmod 600 ~/.agentd/.env
```

Then set `DEEPSEEK_API_KEY` in `~/.agentd/.env`. The DeepSeek loop reads this path by default. A project-local `.env` is still supported as a development fallback, but real secrets should live outside the repository.

Then run:

```bash
cargo run
```

In another terminal:

```bash
python3 scripts/deepseek_agent_loop.py
```

The loop registers a four-node DAG:

1. `architecture_lead`
2. `performance_guardian`
3. `reliability_reviewer`
4. `release_synthesiser`

Two workers coordinate through `agentd`; the daemon controls dependency readiness and state transitions. The script defaults to `deepseek-v4-flash`, disables thinking mode for V4 models, and keeps `max_tokens` low to control cost.

Useful overrides:

```bash
DEEPSEEK_MODEL=deepseek-v4-flash \
DEEPSEEK_MAX_TOKENS=120 \
AGENTD_AGENT_WORKERS=2 \
python3 scripts/deepseek_agent_loop.py
```

To use a different env file:

```bash
AGENTD_ENV_FILE=/path/to/env python3 scripts/deepseek_agent_loop.py
```

## Development Checks

```bash
cargo fmt --check
cargo check
cargo test
cargo package --locked
```

## Release Packaging

GitHub Actions runs CI on pushes and pull requests. Release artifacts are produced by pushing a tag:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The release workflow builds Linux and macOS binaries, packs `.tar.gz` archives with checksums, and publishes them to a GitHub Release.

## Production Direction

The MVP is intentionally small. The next production-hardening slices are:

- Multi-version migration coverage as the schema evolves beyond v1
- Lease owner IDs in richer diagnostics
- Backpressure beyond the current bounded 1 MiB request lines
- Integration tests that launch the daemon and exercise concurrent workers
- Optional Prometheus text export or systemd watchdog integration
