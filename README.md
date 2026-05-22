# agentd

`agentd` is a local-first state machine daemon for AI agents. It runs out of process, stores task state in SQLite, and exposes a Unix Domain Socket (UDS) JSON Lines API so agent runtimes can coordinate long-running DAG work without becoming the source of truth.

## Features

- Durable SQLite state under `~/.agentd`
- Strict DAG node states: `PENDING`, `RUNNING`, `COMPLETED`, `FAILED`
- Lease-based node acquisition with heartbeat and timeout rollback
- Append-only event journal
- Runtime interface discovery: `DescribeInterface`
- Health and metrics endpoints: `Health`, `Metrics`
- Versioned schema migrations
- Real DeepSeek multi-agent loop example
- CI, tests, and release packaging

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
| `DEEPSEEK_MODEL` | `deepseek-v4-flash` |
| `DEEPSEEK_MAX_TOKENS` | `120` |
| `DEEPSEEK_TEMPERATURE` | `0.2` |
| `AGENTD_AGENT_WORKERS` | `2` |

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

Agents should call `DescribeInterface` when they do not have a generated or built-in client.

## Node Leases

`AcquireNextNode` returns:

- `node_id`
- `lease_id`
- `lease_owner`
- `lease_expires_at`
- synthesised execution context

Workers must pass the current `lease_id` to `CommitEvent`, `HeartbeatNode`, `CompleteNode`, and `FailNode`. If a worker stalls past `AGENTD_NODE_TIMEOUT_SECS`, the daemon rolls the node back to `PENDING`; a later worker gets a new lease, and stale lease writes are rejected.

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

The DeepSeek loop registers a four-node DAG: architecture, performance, reliability review, and release synthesis. It uses `deepseek-v4-flash`, bounded prompts, and low token defaults.

## Development Checks

```bash
cargo fmt --check
cargo check --locked
cargo clippy --locked --all-targets -- -D warnings
cargo test --locked
cargo package --locked
python3 -m py_compile client_test.py scripts/deepseek_agent_loop.py
```

## Release

CI runs on pushes and pull requests. Release artifacts are built from tags:

```bash
git tag v0.1.0
git push origin v0.1.0
```

The release workflow builds Linux and macOS binaries, packs `.tar.gz` archives, writes SHA-256 checksums, and publishes a GitHub Release.

## Remaining Hardening

- Multi-version migration tests as schema evolves
- Richer lease diagnostics
- Backpressure beyond bounded 1 MiB request lines
- Optional Prometheus text export or systemd watchdog integration
