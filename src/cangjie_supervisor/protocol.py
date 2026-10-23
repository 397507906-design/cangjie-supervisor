from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any
import uuid

PROTOCOL_VERSION = "cangjie-worker/0.1"


class ProtocolError(ValueError):
    pass


class MessageType(str, Enum):
    HELLO = "HELLO"
    CLAIM = "CLAIM"
    LEASE_GRANT = "LEASE_GRANT"
    HEARTBEAT = "HEARTBEAT"
    PROGRESS = "PROGRESS"
    COMPLETE = "COMPLETE"
    FAILURE = "FAILURE"
    CONTROL = "CONTROL"


class ControlAction(str, Enum):
    HOLD = "HOLD"
    PAUSE = "PAUSE"
    CANCEL = "CANCEL"
    FENCE = "FENCE"


@dataclass(frozen=True)
class Envelope:
    message_type: MessageType
    worker_id: str
    body: dict[str, Any]
    message_id: str = ""
    protocol_version: str = PROTOCOL_VERSION

    def __post_init__(self) -> None:
        if not self.worker_id:
            raise ProtocolError("worker_id is required")
        if self.protocol_version != PROTOCOL_VERSION:
            raise ProtocolError(
                f"unsupported protocol_version: {self.protocol_version}"
            )
        if not self.message_id:
            object.__setattr__(self, "message_id", str(uuid.uuid4()))

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["message_type"] = self.message_type.value
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Envelope":
        required = {"message_type", "worker_id", "body"}
        missing = required.difference(data)
        if missing:
            raise ProtocolError(f"missing fields: {sorted(missing)}")
        return cls(
            message_type=MessageType(data["message_type"]),
            worker_id=str(data["worker_id"]),
            body=dict(data["body"]),
            message_id=str(data.get("message_id") or ""),
            protocol_version=str(data.get("protocol_version") or ""),
        )


@dataclass(frozen=True)
class LeaseRef:
    workflow_id: str
    job_id: str
    lease_token: str
    lease_generation: int

    def validate(self) -> None:
        if not all((self.workflow_id, self.job_id, self.lease_token)):
            raise ProtocolError("incomplete lease reference")
        if self.lease_generation < 1:
            raise ProtocolError("lease_generation must be >= 1")

    def to_dict(self) -> dict[str, Any]:
        self.validate()
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "LeaseRef":
        try:
            ref = cls(
                workflow_id=str(data["workflow_id"]),
                job_id=str(data["job_id"]),
                lease_token=str(data["lease_token"]),
                lease_generation=int(data["lease_generation"]),
            )
        except (KeyError, TypeError, ValueError) as exc:
            raise ProtocolError("invalid lease reference") from exc
        ref.validate()
        return ref


def hello(worker_id: str, capabilities: list[str]) -> Envelope:
    return Envelope(
        MessageType.HELLO,
        worker_id,
        {"capabilities": sorted(set(capabilities))},
    )


def claim(worker_id: str, job_id: str) -> Envelope:
    return Envelope(
        MessageType.CLAIM,
        worker_id,
        {"job_id": job_id},
    )


def lease_grant(
    worker_id: str,
    lease: LeaseRef,
    expires_at: str,
    idempotent: bool = False,
) -> Envelope:
    return Envelope(
        MessageType.LEASE_GRANT,
        worker_id,
        {
            "lease": lease.to_dict(),
            "expires_at": expires_at,
            "idempotent": idempotent,
        },
    )


def heartbeat(worker_id: str, lease: LeaseRef) -> Envelope:
    return Envelope(
        MessageType.HEARTBEAT,
        worker_id,
        {"lease": lease.to_dict()},
    )


def progress(
    worker_id: str,
    lease: LeaseRef,
    checkpoint: str | None = None,
    metrics: dict[str, Any] | None = None,
) -> Envelope:
    return Envelope(
        MessageType.PROGRESS,
        worker_id,
        {
            "lease": lease.to_dict(),
            "checkpoint": checkpoint,
            "metrics": metrics or {},
        },
    )


def complete(
    worker_id: str,
    lease: LeaseRef,
    result: dict[str, Any] | None = None,
) -> Envelope:
    return Envelope(
        MessageType.COMPLETE,
        worker_id,
        {"lease": lease.to_dict(), "result": result or {}},
    )


def failure(
    worker_id: str,
    lease: LeaseRef,
    failure_class: str,
    message: str,
) -> Envelope:
    return Envelope(
        MessageType.FAILURE,
        worker_id,
        {
            "lease": lease.to_dict(),
            "failure_class": failure_class,
            "message": message,
        },
    )


def control(
    worker_id: str,
    action: ControlAction,
    lease: LeaseRef | None = None,
) -> Envelope:
    body: dict[str, Any] = {"action": action.value}
    if lease is not None:
        body["lease"] = lease.to_dict()
    return Envelope(MessageType.CONTROL, worker_id, body)
