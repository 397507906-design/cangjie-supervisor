from __future__ import annotations

import json
import urllib.error
import urllib.request
from dataclasses import dataclass
from typing import Any

from .protocol import (
    PROTOCOL_VERSION,
    Envelope,
    LeaseRef,
    MessageType,
    claim,
    complete,
    failure,
    heartbeat,
    hello,
    progress,
)


class RemoteWorkerError(RuntimeError):
    pass


class RemoteWorkerRejected(RemoteWorkerError):
    def __init__(self, status_code: int, message: str) -> None:
        self.status_code = status_code
        super().__init__(f"HTTP {status_code}: {message}")


class RemoteWorkerNetworkError(RemoteWorkerError):
    pass


@dataclass(frozen=True)
class ClaimResult:
    lease: LeaseRef
    expires_at: str
    idempotent: bool


class RemoteWorkerClient:
    def __init__(
        self,
        server_url: str,
        worker_token: str,
        worker_id: str,
        timeout: float = 3.0,
    ) -> None:
        self.server_url = server_url.rstrip("/")
        self.worker_token = worker_token
        self.worker_id = worker_id
        self.timeout = timeout
        if not self.server_url or not self.worker_token or not self.worker_id:
            raise ValueError("server_url, worker_token and worker_id are required")
    def _request(
        self,
        path: str,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        data = None
        method = "GET"
        headers = {
            "Authorization": f"Bearer {self.worker_token}",
        }
        if payload is not None:
            method = "POST"
            data = json.dumps(payload).encode()
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            self.server_url + path,
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                body = json.loads(response.read().decode())
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode()
            try:
                body = json.loads(raw)
                message = str(body.get("error") or body)
            except Exception:
                message = raw or str(exc)
            raise RemoteWorkerRejected(exc.code, message) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise RemoteWorkerNetworkError(f"network error: {exc}") from exc
        except json.JSONDecodeError as exc:
            raise RemoteWorkerError("server returned invalid JSON") from exc
        if not isinstance(body, dict):
            raise RemoteWorkerError("server response must be a JSON object")
        return body
    def health(self) -> dict[str, Any]:
        body = self._request("/health")
        if body.get("protocol_version") != PROTOCOL_VERSION:
            raise RemoteWorkerError(
                f"protocol mismatch: {body.get('protocol_version')!r}"
            )
        return body

    def send(self, envelope: Envelope) -> dict[str, Any]:
        if envelope.worker_id != self.worker_id:
            raise RemoteWorkerError("envelope worker_id does not match client")
        return self._request(
            "/v1/worker/message",
            envelope.to_dict(),
        )

    def hello(self, capabilities: list[str]) -> dict[str, Any]:
        return self.send(hello(self.worker_id, capabilities))

    def claim(self, job_id: str) -> ClaimResult:
        body = self.send(claim(self.worker_id, job_id))
        envelope = Envelope.from_dict(body)
        if envelope.message_type != MessageType.LEASE_GRANT:
            raise RemoteWorkerError(
                f"expected LEASE_GRANT, got {envelope.message_type.value}"
            )
        lease = LeaseRef.from_dict(dict(envelope.body.get("lease") or {}))
        return ClaimResult(
            lease=lease,
            expires_at=str(envelope.body["expires_at"]),
            idempotent=bool(envelope.body.get("idempotent", False)),
        )

    def heartbeat(self, lease: LeaseRef) -> str:
        body = self.send(heartbeat(self.worker_id, lease))
        return str(body["expires_at"])
    def progress(
        self,
        lease: LeaseRef,
        checkpoint: str | None = None,
        metrics: dict[str, Any] | None = None,
    ) -> None:
        self.send(
            progress(
                self.worker_id,
                lease,
                checkpoint=checkpoint,
                metrics=metrics,
            )
        )

    def complete(
        self,
        lease: LeaseRef,
        result: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        return self.send(
            complete(
                self.worker_id,
                lease,
                result=result,
            )
        )

    def fail(
        self,
        lease: LeaseRef,
        failure_class: str,
        message: str,
    ) -> dict[str, Any]:
        return self.send(
            failure(
                self.worker_id,
                lease,
                failure_class,
                message,
            )
        )
