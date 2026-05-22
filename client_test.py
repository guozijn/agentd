#!/usr/bin/env python3
import json
import os
import socket
import uuid


SOCKET_PATH = os.environ.get("AGENTD_SOCKET_PATH", "/tmp/agentd.sock")


def call(sock_file, request_id, method, params):
    request = {"id": request_id, "method": method, "params": params}
    sock_file.write(json.dumps(request).encode("utf-8") + b"\n")
    sock_file.flush()
    response = json.loads(sock_file.readline().decode("utf-8"))
    if "error" in response:
        raise RuntimeError(response["error"])
    return response["result"]


def main():
    first_node = str(uuid.uuid4())
    second_node = str(uuid.uuid4())

    with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as sock:
        sock.connect(SOCKET_PATH)
        sock_file = sock.makefile("rwb")

        registered = call(
            sock_file,
            1,
            "RegisterTask",
            {
                "goal": "Demonstrate agentd node acquisition",
                "context": {"source": "client_test.py"},
                "initial_nodes": [
                    {
                        "id": first_node,
                        "dependencies": [],
                        "payload_schema": {"type": "object"},
                    },
                    {
                        "id": second_node,
                        "dependencies": [first_node],
                        "payload_schema": {"type": "object"},
                    },
                ],
            },
        )
        task_id = registered["task_id"]
        print("registered task:", task_id)

        acquired = call(sock_file, 2, "AcquireNextNode", {"task_id": task_id})
        print("first acquire:", json.dumps(acquired, indent=2))

        node_id = acquired["node_id"]
        lease_id = acquired["lease_id"]
        event = call(
            sock_file,
            3,
            "CommitEvent",
            {
                "task_id": task_id,
                "node_id": node_id,
                "lease_id": lease_id,
                "payload": {"message": "node started"},
            },
        )
        print("committed event:", event["event_id"])

        completed = call(
            sock_file,
            4,
            "CompleteNode",
            {
                "task_id": task_id,
                "node_id": node_id,
                "lease_id": lease_id,
                "result_payload": {"ok": True, "value": "first node complete"},
            },
        )
        print("completed node:", completed["event_id"])

        acquired = call(sock_file, 5, "AcquireNextNode", {"task_id": task_id})
        print("second acquire:", json.dumps(acquired, indent=2))


if __name__ == "__main__":
    main()
