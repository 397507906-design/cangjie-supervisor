from __future__ import annotations

import argparse
import hmac
import ipaddress
import json
import socket
import threading
import uuid
from dataclasses import dataclass
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from .core import Supervisor, SupervisorError
from .protocol import (
    PROTOCOL_VERSION,
    Envelope,
    LeaseRef,
    MessageType,
    ProtocolError,
    lease_grant,
)


@dataclass(frozen=True)
class ServiceConfig:
    db_path: str
    workflow_id: str
    bind_host: str
    port: int
    worker_token: str
    operator_token: str
    lease_seconds: float = 6.0
    runtime_ttl_seconds: float = 9.0
    @classmethod
    def from_file(cls, path: str | Path) -> "ServiceConfig":
        data = json.loads(Path(path).read_text())
        required = {
            "db_path",
            "workflow_id",
            "bind_host",
            "port",
            "worker_token",
            "operator_token",
        }
        missing = required.difference(data)
        if missing:
            raise ValueError(f"missing service config fields: {sorted(missing)}")
        cfg = cls(
            db_path=str(data["db_path"]),
            workflow_id=str(data["workflow_id"]),
            bind_host=str(data["bind_host"]),
            port=int(data["port"]),
            worker_token=str(data["worker_token"]),
            operator_token=str(data["operator_token"]),
            lease_seconds=float(data.get("lease_seconds", 6.0)),
            runtime_ttl_seconds=float(data.get("runtime_ttl_seconds", 9.0)),
        )
        cfg.validate()
        return cfg

    def validate(self) -> None:
        if not self.worker_token or not self.operator_token:
            raise ValueError("worker_token and operator_token are required")
        if self.worker_token == self.operator_token:
            raise ValueError("worker_token and operator_token must differ")
        if not 0 <= self.port <= 65535:
            raise ValueError("invalid port")
        if self.lease_seconds <= 0 or self.runtime_ttl_seconds <= 0:
            raise ValueError("lease durations must be positive")


def _token_matches(header: str | None, expected: str) -> bool:
    if not header or not header.startswith("Bearer "):
        return False
    return hmac.compare_digest(header[7:], expected)


def _is_loopback(address: str) -> bool:
    try:
        return ipaddress.ip_address(address).is_loopback
    except ValueError:
        return False
class SupervisorGateway:
    def __init__(self, config: ServiceConfig) -> None:
        self.config = config

    def open(self) -> Supervisor:
        return Supervisor(
            self.config.db_path,
            workflow_id=self.config.workflow_id,
        )

    def worker_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        envelope = Envelope.from_dict(payload)
        if envelope.message_type == MessageType.HELLO:
            with self.open() as sup:
                status = sup.status()
            meta = status["supervisor"]
            available_job_id = (
                meta["authorized_job_id"]
                if meta["state"] == "AUTHORIZED"
                else None
            )
            return {
                "ok": True,
                "protocol_version": PROTOCOL_VERSION,
                "worker_id": envelope.worker_id,
                "available_job_id": available_job_id,
            }
        if envelope.message_type == MessageType.CLAIM:
            return self._claim(envelope)
        if envelope.message_type in {
            MessageType.HEARTBEAT,
            MessageType.PROGRESS,
            MessageType.COMPLETE,
            MessageType.FAILURE,
        }:
            return self._lease_message(envelope)
        raise ProtocolError(
            f"worker cannot send message_type={envelope.message_type.value}"
        )

    def _claim(self, envelope: Envelope) -> dict[str, Any]:
        job_id = str(envelope.body.get("job_id") or "")
        if not job_id:
            raise ProtocolError("CLAIM requires job_id")
        with self.open() as sup:
            lease = sup.claim(
                job_id,
                envelope.worker_id,
                lease_seconds=self.config.lease_seconds,
            )
        ref = LeaseRef(
            workflow_id=self.config.workflow_id,
            job_id=job_id,
            lease_token=lease["lease_token"],
            lease_generation=int(lease["generation"]),
        )
        return lease_grant(
            envelope.worker_id,
            ref,
            lease["expires_at"],
            idempotent=bool(lease.get("idempotent", False)),
        ).to_dict()
    def _lease_message(self, envelope: Envelope) -> dict[str, Any]:
        ref = LeaseRef.from_dict(dict(envelope.body.get("lease") or {}))
        if ref.workflow_id != self.config.workflow_id:
            raise ProtocolError("lease workflow_id mismatch")
        with self.open() as sup:
            if envelope.message_type == MessageType.HEARTBEAT:
                expires_at = sup.heartbeat(
                    ref.job_id,
                    ref.lease_token,
                    lease_seconds=self.config.lease_seconds,
                    lease_generation=ref.lease_generation,
                )
                return {"ok": True, "expires_at": expires_at}

            if envelope.message_type == MessageType.PROGRESS:
                sup.record_progress(
                    ref.job_id,
                    ref.lease_token,
                    checkpoint=envelope.body.get("checkpoint"),
                    metrics=dict(envelope.body.get("metrics") or {}),
                    actor=envelope.worker_id,
                    lease_generation=ref.lease_generation,
                )
                return {"ok": True}

            if envelope.message_type == MessageType.COMPLETE:
                sup.complete(
                    ref.job_id,
                    ref.lease_token,
                    actor=envelope.worker_id,
                    lease_generation=ref.lease_generation,
                )
                return {"ok": True, "state": "HOLD"}

            if envelope.message_type == MessageType.FAILURE:
                sup.fail(
                    ref.job_id,
                    ref.lease_token,
                    str(envelope.body.get("failure_class") or ""),
                    str(envelope.body.get("message") or ""),
                    actor=envelope.worker_id,
                    lease_generation=ref.lease_generation,
                )
                return {"ok": True, "state": "FAILED"}

        raise ProtocolError("unsupported worker message")
    def status(self) -> dict[str, Any]:
        with self.open() as sup:
            return sup.status()

    def audit(self, limit: int) -> list[dict[str, Any]]:
        with self.open() as sup:
            return sup.audit(limit)

    def submit(self, body: dict[str, Any]) -> dict[str, Any]:
        job_id = str(body.get("job_id") or "")
        if not job_id:
            raise ValueError("job_id is required")
        payload = dict(body.get("payload") or {})
        with self.open() as sup:
            return sup.submit_job(job_id, payload, actor="operator-service")

    def command(self, body: dict[str, Any]) -> dict[str, Any]:
        command = str(body.get("command") or "").upper()
        command_id = body.get("command_id")
        job_id = body.get("job_id")
        with self.open() as sup:
            if command == "AUTHORIZE":
                if not job_id:
                    raise ValueError("AUTHORIZE requires job_id")
                sup.authorize(
                    str(job_id),
                    actor="operator-service",
                    command_id=command_id,
                )
            elif command == "HOLD":
                sup.hold(actor="operator-service", command_id=command_id)
            elif command == "PAUSE":
                sup.pause(actor="operator-service", command_id=command_id)
            elif command == "RESUME":
                sup.resume(actor="operator-service", command_id=command_id)
            elif command == "CANCEL":
                sup.cancel(actor="operator-service", command_id=command_id)
            elif command == "RETRY":
                sup.retry(actor="operator-service", command_id=command_id)
            else:
                raise ValueError(f"unsupported operator command: {command}")
            return sup.status()
class SupervisorHTTPServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(
        self,
        server_address: tuple[str, int],
        gateway: SupervisorGateway,
    ) -> None:
        self.gateway = gateway
        super().__init__(server_address, SupervisorHandler)


class SupervisorHandler(BaseHTTPRequestHandler):
    server_version = "CangjieSupervisor/0.2"

    @property
    def gateway(self) -> SupervisorGateway:
        return self.server.gateway  # type: ignore[attr-defined]

    def log_message(self, fmt: str, *args: object) -> None:
        return

    def _json(self, code: int, value: object) -> None:
        data = json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
        ).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(length) if length else b"{}"
        value = json.loads(raw.decode())
        if not isinstance(value, dict):
            raise ValueError("JSON body must be an object")
        return value
    def _operator_allowed(self) -> bool:
        return (
            _is_loopback(self.client_address[0])
            and _token_matches(
                self.headers.get("Authorization"),
                self.gateway.config.operator_token,
            )
        )

    def _worker_allowed(self) -> bool:
        return _token_matches(
            self.headers.get("Authorization"),
            self.gateway.config.worker_token,
        )

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/health":
                self._json(
                    200,
                    {
                        "ok": True,
                        "protocol_version": PROTOCOL_VERSION,
                        "workflow_id": self.gateway.config.workflow_id,
                    },
                )
                return

            if parsed.path == "/v1/operator/status":
                if not self._operator_allowed():
                    self._json(403, {"error": "operator access denied"})
                    return
                self._json(200, self.gateway.status())
                return

            if parsed.path == "/v1/operator/audit":
                if not self._operator_allowed():
                    self._json(403, {"error": "operator access denied"})
                    return
                query = parse_qs(parsed.query)
                limit = int(query.get("limit", ["50"])[0])
                self._json(200, self.gateway.audit(limit))
                return

            self._json(404, {"error": "not found"})
        except (ValueError, ProtocolError) as exc:
            self._json(400, {"error": str(exc)})
        except SupervisorError as exc:
            self._json(409, {"error": str(exc)})
    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/v1/worker/message":
                if not self._worker_allowed():
                    self._json(401, {"error": "worker authentication failed"})
                    return
                self._json(200, self.gateway.worker_message(self._body()))
                return

            if parsed.path == "/v1/operator/submit":
                if not self._operator_allowed():
                    self._json(403, {"error": "operator access denied"})
                    return
                self._json(200, self.gateway.submit(self._body()))
                return

            if parsed.path == "/v1/operator/command":
                if not self._operator_allowed():
                    self._json(403, {"error": "operator access denied"})
                    return
                self._json(200, self.gateway.command(self._body()))
                return

            self._json(404, {"error": "not found"})
        except json.JSONDecodeError:
            self._json(400, {"error": "invalid JSON"})
        except (ValueError, ProtocolError) as exc:
            self._json(400, {"error": str(exc)})
        except SupervisorError as exc:
            self._json(409, {"error": str(exc)})
        except Exception as exc:
            self._json(
                500,
                {"error": type(exc).__name__, "message": str(exc)},
            )
def build_server(config: ServiceConfig) -> SupervisorHTTPServer:
    config.validate()
    gateway = SupervisorGateway(config)
    return SupervisorHTTPServer((config.bind_host, config.port), gateway)


def run_service(config: ServiceConfig) -> None:
    config.validate()
    owner = f"service-{socket.gethostname()}-{uuid.uuid4().hex[:8]}"
    with Supervisor(config.db_path, workflow_id=config.workflow_id) as sup:
        runtime_token = sup.acquire_instance(
            owner,
            ttl_seconds=config.runtime_ttl_seconds,
        )

    server = build_server(config)
    stop = threading.Event()
    watchdog_error: list[BaseException] = []

    def watchdog() -> None:
        interval = max(0.25, config.runtime_ttl_seconds / 3)
        try:
            with Supervisor(
                config.db_path,
                workflow_id=config.workflow_id,
            ) as sup:
                while not stop.wait(interval):
                    sup.renew_instance(
                        runtime_token,
                        ttl_seconds=config.runtime_ttl_seconds,
                    )
                    sup.reconcile()
        except BaseException as exc:
            watchdog_error.append(exc)
            server.shutdown()

    thread = threading.Thread(
        target=watchdog,
        name="cangjie-supervisor-watchdog",
        daemon=True,
    )
    thread.start()
    try:
        print(
            f"Cangjie Supervisor service listening on "
            f"{config.bind_host}:{server.server_address[1]}",
            flush=True,
        )
        server.serve_forever(poll_interval=0.25)
    finally:
        stop.set()
        server.server_close()
        thread.join(timeout=2)
        try:
            with Supervisor(
                config.db_path,
                workflow_id=config.workflow_id,
            ) as sup:
                sup.release_instance(runtime_token, owner)
        except SupervisorError:
            pass

    if watchdog_error:
        raise SupervisorError(
            f"runtime watchdog stopped service: {watchdog_error[0]}"
        )
def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="cangjie-supervisor-service")
    p.add_argument("--config", required=True)
    return p


def main() -> None:
    args = parser().parse_args()
    try:
        config = ServiceConfig.from_file(args.config)
        run_service(config)
    except KeyboardInterrupt:
        pass
    except (OSError, ValueError, SupervisorError) as exc:
        print(
            json.dumps({"error": str(exc)}, ensure_ascii=False),
            flush=True,
        )
        raise SystemExit(2)


if __name__ == "__main__":
    main()
