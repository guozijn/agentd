#!/usr/bin/env python3
import argparse
import json
import os
import queue
import socket
import threading
import time
import urllib.error
import urllib.request
import uuid


DEFAULT_SOCKET_PATH = os.path.join(os.path.expanduser("~"), ".agentd", "agentd.sock")
DEFAULT_BASE_URL = "https://api.deepseek.com"
DEFAULT_MODEL = "deepseek-v4-flash"
DEFAULT_ENV_FILE = os.path.join(os.path.expanduser("~"), ".agentd", ".env")


class RpcClient:
    def __init__(self, socket_path):
        self.socket_path = socket_path
        self._request_id = 0
        self._lock = threading.Lock()

    def call(self, method, params):
        with self._lock:
            self._request_id += 1
            request_id = self._request_id

        request = {"id": request_id, "method": method, "params": params}
        with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
            sock.connect(self.socket_path)
            sock_file = sock.makefile("rwb")
            sock_file.write(json.dumps(request).encode("utf-8") + b"\n")
            sock_file.flush()
            response_line = sock_file.readline()

        response = json.loads(response_line.decode("utf-8"))
        if response.get("error"):
            raise RuntimeError(response["error"])
        return response["result"]


def load_dotenv(path=".env"):
    if not os.path.exists(path):
        return

    with open(path, "r", encoding="utf-8") as handle:
        for raw_line in handle:
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip().strip('"').strip("'")
            os.environ.setdefault(key, value)


def deepseek_chat(api_key, base_url, model, messages, max_tokens, temperature):
    body = {
        "model": model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": temperature,
    }
    if model.startswith("deepseek-v4"):
        body["thinking"] = {"type": "disabled"}

    request = urllib.request.Request(
        f"{base_url.rstrip('/')}/chat/completions",
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=45) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as err:
        error_body = err.read().decode("utf-8", errors="replace")[:600]
        raise RuntimeError(f"DeepSeek HTTP {err.code}: {error_body}") from err

    message = payload["choices"][0]["message"]
    return {
        "model": payload.get("model", model),
        "content": message.get("content", ""),
        "finish_reason": payload["choices"][0].get("finish_reason"),
        "usage": payload.get("usage", {}),
    }


def build_agent_nodes():
    architecture_id = str(uuid.uuid4())
    performance_id = str(uuid.uuid4())
    reviewer_id = str(uuid.uuid4())
    synthesiser_id = str(uuid.uuid4())

    return [
        {
            "id": architecture_id,
            "dependencies": [],
            "payload_schema": {
                "agent": "architecture_lead",
                "objective": "Identify the minimum production architecture changes for a local-first AI agent state daemon.",
                "output_contract": {
                    "summary": "one concise paragraph",
                    "state_invariants": "short list",
                    "next_actions": "short list",
                },
            },
        },
        {
            "id": performance_id,
            "dependencies": [],
            "payload_schema": {
                "agent": "performance_guardian",
                "objective": "Minimise resource cost for long-running local AI orchestration while preserving crash resilience.",
                "output_contract": {
                    "summary": "one concise paragraph",
                    "cost_controls": "short list",
                    "risks": "short list",
                },
            },
        },
        {
            "id": reviewer_id,
            "dependencies": [architecture_id, performance_id],
            "payload_schema": {
                "agent": "reliability_reviewer",
                "objective": "Review dependency outputs and find reliability gaps before this daemon is used by multiple agents.",
                "output_contract": {
                    "summary": "one concise paragraph",
                    "blocking_gaps": "short list",
                    "acceptance_checks": "short list",
                },
            },
        },
        {
            "id": synthesiser_id,
            "dependencies": [reviewer_id],
            "payload_schema": {
                "agent": "release_synthesiser",
                "objective": "Synthesize a production-readiness verdict and the smallest next implementation slice.",
                "output_contract": {
                    "verdict": "ship or hold",
                    "evidence": "short list",
                    "next_slice": "short list",
                },
            },
        },
    ]


def build_messages(acquired):
    context = acquired["context"]
    node = context["node"]
    task = context["task"]
    payload = node["payload_schema"]
    dependency_results = context.get("completed_dependencies", [])

    system = (
        "You are one specialised agent in a coordinated local-first agent infrastructure test. "
        "Be concrete, terse, and operational. Return valid compact JSON only."
    )
    user = {
        "task_goal": task["goal"],
        "task_context": task["context"],
        "your_agent": payload,
        "completed_dependency_results": dependency_results,
        "instructions": [
            "Do not ask follow-up questions.",
            "Prefer low-cost, high-performance infrastructure decisions.",
            "Keep the response under 140 words.",
        ],
    }
    return [
        {"role": "system", "content": system},
        {"role": "user", "content": json.dumps(user, separators=(",", ":"))},
    ]


def run_worker(name, rpc, task_id, api_key, base_url, model, max_tokens, temperature, done):
    while not done.is_set():
        acquired = rpc.call("AcquireNextNode", {"task_id": task_id, "lease_owner": name})
        if acquired is None:
            status = rpc.call("TaskStatus", {"task_id": task_id})
            counts = status["counts"]
            total = len(status["nodes"])
            terminal = counts.get("COMPLETED", 0) + counts.get("FAILED", 0)
            if terminal >= total:
                done.set()
                return
            time.sleep(0.25)
            continue

        node_id = acquired["node_id"]
        lease_id = acquired["lease_id"]
        agent = acquired["context"]["node"]["payload_schema"].get("agent", "unknown")
        started_at = time.time()
        print(f"{name} acquired {agent} ({node_id})", flush=True)

        try:
            rpc.call(
                "CommitEvent",
                {
                    "task_id": task_id,
                    "node_id": node_id,
                    "lease_id": lease_id,
                    "action_type": "DEEPSEEK_REQUEST_STARTED",
                    "payload": {"agent": agent, "model": model},
                },
            )
            rpc.call(
                "HeartbeatNode",
                {"task_id": task_id, "node_id": node_id, "lease_id": lease_id},
            )
            result = deepseek_chat(
                api_key,
                base_url,
                model,
                build_messages(acquired),
                max_tokens,
                temperature,
            )
            rpc.call(
                "HeartbeatNode",
                {"task_id": task_id, "node_id": node_id, "lease_id": lease_id},
            )
            rpc.call(
                "CommitEvent",
                {
                    "task_id": task_id,
                    "node_id": node_id,
                    "lease_id": lease_id,
                    "action_type": "DEEPSEEK_RESPONSE",
                    "payload": {
                        "agent": agent,
                        "latency_ms": int((time.time() - started_at) * 1000),
                        "usage": result.get("usage", {}),
                    },
                },
            )
            rpc.call(
                "CompleteNode",
                {
                    "task_id": task_id,
                    "node_id": node_id,
                    "lease_id": lease_id,
                    "result_payload": {
                        "agent": agent,
                        "model": result["model"],
                        "content": result["content"],
                        "finish_reason": result.get("finish_reason"),
                        "usage": result.get("usage", {}),
                    },
                },
            )
            print(f"{name} completed {agent}", flush=True)
        except Exception as exc:
            message = str(exc)[:800]
            try:
                rpc.call(
                    "FailNode",
                    {
                        "task_id": task_id,
                        "node_id": node_id,
                        "lease_id": lease_id,
                        "error_payload": {"agent": agent, "error": message},
                    },
                )
            finally:
                done.set()
                raise


def run(args):
    load_dotenv(args.env_file)
    api_key = os.environ.get("DEEPSEEK_API_KEY")
    if not api_key:
        raise SystemExit("DEEPSEEK_API_KEY is not set; add it to .env or the environment")

    rpc = RpcClient(args.socket_path)
    registered = rpc.call(
        "RegisterTask",
        {
            "goal": "Coordinate specialised AI agents to assess and improve agentd as production local-first infrastructure.",
            "context": {
                "repository": "agentd",
                "cost_policy": "Use the flash model, short prompts, bounded context, and low max_tokens.",
                "daemon_role": "Single source of truth for agent task state and strict DAG transitions.",
            },
            "initial_nodes": build_agent_nodes(),
        },
    )
    task_id = registered["task_id"]
    print(f"registered task: {task_id}", flush=True)

    done = threading.Event()
    errors = queue.Queue()
    threads = []
    for index in range(args.workers):
        thread = threading.Thread(
            target=lambda worker_index=index: guarded_worker(
                worker_index,
                errors,
                done,
                rpc,
                task_id,
                api_key,
                args.base_url,
                args.model,
                args.max_tokens,
                args.temperature,
            ),
            daemon=True,
        )
        threads.append(thread)
        thread.start()

    for thread in threads:
        thread.join()

    if not errors.empty():
        raise SystemExit(errors.get())

    status = rpc.call("TaskStatus", {"task_id": task_id})
    print(json.dumps(status, indent=2), flush=True)
    failed = status["counts"].get("FAILED", 0)
    if failed:
        raise SystemExit(f"{failed} node(s) failed")


def guarded_worker(
    worker_index,
    errors,
    done,
    rpc,
    task_id,
    api_key,
    base_url,
    model,
    max_tokens,
    temperature,
):
    try:
        run_worker(
            f"worker-{worker_index + 1}",
            rpc,
            task_id,
            api_key,
            base_url,
            model,
            max_tokens,
            temperature,
            done,
        )
    except Exception as exc:
        errors.put(str(exc))
        done.set()


def parse_args():
    parser = argparse.ArgumentParser(
        description="Run a real DeepSeek-backed multi-agent loop against agentd."
    )
    parser.add_argument(
        "--socket-path", default=os.environ.get("AGENTD_SOCKET_PATH", DEFAULT_SOCKET_PATH)
    )
    parser.add_argument("--env-file", default=default_env_file())
    parser.add_argument(
        "--base-url", default=os.environ.get("DEEPSEEK_BASE_URL", DEFAULT_BASE_URL)
    )
    parser.add_argument("--model", default=os.environ.get("DEEPSEEK_MODEL", DEFAULT_MODEL))
    parser.add_argument("--max-tokens", type=int, default=int(os.environ.get("DEEPSEEK_MAX_TOKENS", "180")))
    parser.add_argument("--temperature", type=float, default=float(os.environ.get("DEEPSEEK_TEMPERATURE", "0.2")))
    parser.add_argument("--workers", type=int, default=int(os.environ.get("AGENTD_AGENT_WORKERS", "2")))
    return parser.parse_args()


def default_env_file():
    configured = os.environ.get("AGENTD_ENV_FILE")
    if configured:
        return configured
    if os.path.exists(DEFAULT_ENV_FILE):
        return DEFAULT_ENV_FILE
    return ".env"


if __name__ == "__main__":
    run(parse_args())
