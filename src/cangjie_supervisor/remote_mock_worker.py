from __future__ import annotations

import argparse
import json
import socket
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from .remote_client import RemoteWorkerClient, RemoteWorkerError


def parse_deadline(value: str) -> float:
    return datetime.fromisoformat(value).astimezone(timezone.utc).timestamp()


def emit(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True), flush=True)


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="cangjie-remote-mock-worker")
    p.add_argument("--config", required=True)
    p.add_argument("job_id")
    p.add_argument("--steps", type=int, default=5)
    p.add_argument("--interval", type=float, default=0.5)
    p.add_argument("--crash-at", type=int)
    return p


def load_config(path: str) -> dict:
    data = json.loads(Path(path).read_text())
    required = {"server_url", "worker_token"}
    missing = required.difference(data)
    if missing:
        raise ValueError(f"missing worker config fields: {sorted(missing)}")
    return data
def main() -> None:
    args = parser().parse_args()
    cfg = load_config(args.config)
    worker_id = str(
        cfg.get("worker_id")
        or f"remote-mock-{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
    )
    client = RemoteWorkerClient(
        str(cfg["server_url"]),
        str(cfg["worker_token"]),
        worker_id,
        timeout=float(cfg.get("timeout", 3.0)),
    )

    try:
        health = client.health()
        emit({"event": "HEALTH", **health})
        client.hello(["simulation", "lease-v1"])
        emit({"event": "HELLO", "worker_id": worker_id})

        claimed = client.claim(args.job_id)
        lease = claimed.lease
        deadline = parse_deadline(claimed.expires_at)
        emit(
            {
                "event": "CLAIMED",
                "job_id": lease.job_id,
                "generation": lease.lease_generation,
                "idempotent": claimed.idempotent,
            }
        )

        for step in range(1, args.steps + 1):
            if time.time() >= deadline:
                emit({"event": "FENCED_LOCAL_DEADLINE", "step": step})
                raise SystemExit(5)

            if args.crash_at == step:
                emit({"event": "SIMULATED_CRASH", "step": step})
                raise SystemExit(37)

            time.sleep(args.interval)
            try:
                expires_at = client.heartbeat(lease)
                deadline = parse_deadline(expires_at)
                client.progress(
                    lease,
                    checkpoint=f"step-{step}",
                    metrics={"step": step},
                )
                emit({"event": "STEP", "step": step})
            except RemoteWorkerError as exc:
                emit(
                    {
                        "event": "RENEWAL_UNAVAILABLE",
                        "step": step,
                        "error": str(exc),
                    }
                )
                if time.time() >= deadline:
                    emit({"event": "FENCED_NETWORK_PARTITION", "step": step})
                    raise SystemExit(6)

        result = client.complete(
            lease,
            {"simulated_steps": args.steps},
        )
        emit({"event": "COMPLETE", **result})
    except RemoteWorkerError as exc:
        emit({"event": "REJECTED", "error": str(exc)})
        raise SystemExit(2)


if __name__ == "__main__":
    main()
