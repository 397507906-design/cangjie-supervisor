"""Local-only test fixture. Production Workers must use the remote protocol."""

from __future__ import annotations

import argparse
import json
import os
import socket
import time
import uuid

from .core import Supervisor, SupervisorError
from .cli import DEFAULT_DB


def emit(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True), flush=True)


def main() -> None:
    p = argparse.ArgumentParser(prog="cangjie-mock-worker")
    p.add_argument("job_id")
    p.add_argument("--db", default=str(DEFAULT_DB))
    p.add_argument("--holder")
    p.add_argument("--steps", type=int, default=10)
    p.add_argument("--interval", type=float, default=0.2)
    p.add_argument("--lease-seconds", type=float, default=2.0)
    p.add_argument("--crash-at", type=int)
    p.add_argument("--fail-at", type=int)
    p.add_argument("--fail-class", default="UNKNOWN_FAILURE")
    args = p.parse_args()
    holder = args.holder or f"mock-{socket.gethostname()}-{uuid.uuid4().hex[:6]}"
    try:
        with Supervisor(args.db) as sup:
            lease = sup.claim(args.job_id, holder, args.lease_seconds)
            token = lease["lease_token"]
            emit({"event": "CLAIMED", **lease, "holder": holder})
            for step in range(1, args.steps + 1):
                if sup.worker_permission(args.job_id, token) != "RUN":
                    emit({"event": "FENCED", "step": step})
                    return
                if args.crash_at == step:
                    emit({"event": "SIMULATED_CRASH", "step": step})
                    os._exit(37)
                if args.fail_at == step:
                    sup.fail(
                        args.job_id,
                        token,
                        args.fail_class,
                        f"simulated failure at step {step}",
                        holder,
                    )
                    emit({"event": "FAILED", "step": step})
                    return
                time.sleep(args.interval)
                if sup.worker_permission(args.job_id, token) != "RUN":
                    emit({"event": "FENCED", "step": step})
                    return
                try:
                    sup.heartbeat(args.job_id, token, args.lease_seconds)
                except SupervisorError:
                    emit({"event": "FENCED", "step": step})
                    return
                emit({"event": "STEP", "step": step})
            sup.complete(args.job_id, token, holder)
            emit({"event": "DONE_TO_HOLD"})
    except SupervisorError as exc:
        emit({"event": "REJECTED", "error": str(exc)})
        raise SystemExit(2)


if __name__ == "__main__":
    main()
