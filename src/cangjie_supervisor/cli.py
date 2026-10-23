from __future__ import annotations

import argparse
import json
import socket
import sys
import time
import uuid
from pathlib import Path

from .core import Supervisor, SupervisorError

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DB = PROJECT_ROOT / "data" / "supervisor.db"


def emit(value: object) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True))


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="cangjie-supervisor")
    p.add_argument("--db", default=str(DEFAULT_DB))
    sub = p.add_subparsers(dest="command", required=True)

    sub.add_parser("init")
    sub.add_parser("status")
    submit = sub.add_parser("submit")
    submit.add_argument("job_id")
    submit.add_argument("--payload", default="{}")

    for name in ("authorize",):
        cmd = sub.add_parser(name)
        cmd.add_argument("job_id")
        cmd.add_argument("--command-id")

    for name in ("hold", "pause", "resume", "cancel", "retry"):
        cmd = sub.add_parser(name)
        cmd.add_argument("--command-id")

    audit = sub.add_parser("audit")
    audit.add_argument("--limit", type=int, default=50)

    advice = sub.add_parser("advisory")
    advice.add_argument("--job-id")
    advice.add_argument("--diagnosis", required=True)
    advice.add_argument("--confidence", type=float, required=True)
    advice.add_argument("--suggested-action", required=True)
    advice.add_argument("--risk", required=True)

    serve = sub.add_parser("serve")
    serve.add_argument("--owner")
    serve.add_argument("--ttl", type=float, default=5.0)
    return p
def run(args: argparse.Namespace) -> int:
    with Supervisor(args.db) as sup:
        if args.command in {"init", "status"}:
            emit(sup.status())
        elif args.command == "submit":
            emit(sup.submit_job(args.job_id, json.loads(args.payload)))
        elif args.command == "authorize":
            sup.authorize(args.job_id, command_id=args.command_id)
            emit(sup.status())
        elif args.command == "hold":
            sup.hold(command_id=args.command_id)
            emit(sup.status())
        elif args.command == "pause":
            sup.pause(command_id=args.command_id)
            emit(sup.status())
        elif args.command == "resume":
            sup.resume(command_id=args.command_id)
            emit(sup.status())
        elif args.command == "cancel":
            sup.cancel(command_id=args.command_id)
            emit(sup.status())
        elif args.command == "retry":
            sup.retry(command_id=args.command_id)
            emit(sup.status())
        elif args.command == "audit":
            emit(sup.audit(args.limit))
        elif args.command == "advisory":
            before = sup.status()["supervisor"]["state"]
            advisory_id = sup.add_advisory(
                diagnosis=args.diagnosis,
                confidence=args.confidence,
                suggested_action=args.suggested_action,
                risk=args.risk,
                job_id=args.job_id,
            )
            after = sup.status()["supervisor"]["state"]
            emit({"advisory_id": advisory_id, "state_before": before, "state_after": after})
        elif args.command == "serve":
            owner = args.owner or f"{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
            token = sup.acquire_instance(owner, args.ttl)
            emit({"runtime": "ACTIVE", "owner": owner, "lease_token": token})
            try:
                while True:
                    time.sleep(max(0.1, args.ttl / 3))
                    sup.renew_instance(token, args.ttl)
                    sup.reconcile()
            except KeyboardInterrupt:
                sup.release_instance(token, owner)
        else:
            raise SupervisorError(f"unknown command: {args.command}")
    return 0


def main() -> None:
    args = parser().parse_args()
    try:
        raise SystemExit(run(args))
    except (SupervisorError, ValueError, json.JSONDecodeError) as exc:
        emit({"error": str(exc)})
        raise SystemExit(2)


if __name__ == "__main__":
    main()
