from __future__ import annotations

import json
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

STATES = {"HOLD", "AUTHORIZED", "RUNNING", "PAUSED", "FAILED"}
FAILURE_CLASSES = {"EXPECTED_GATE", "KNOWN_RECOVERABLE", "UNKNOWN_FAILURE"}
JOB_STATES = {"QUEUED", "READY", "RUNNING", "PAUSED", "FAILED", "DONE", "CANCELLED"}


class SupervisorError(RuntimeError):
    pass


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(dt: datetime | None = None) -> str:
    return (dt or utcnow()).isoformat()
class Supervisor:
    """Persistent, fail-closed control plane. No executor integration in v0.1."""

    def __init__(
        self,
        db_path: str | Path,
        workflow_id: str = "cangjie-supervisor-v0.1-demo",
    ) -> None:
        self.db_path = str(db_path)
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, timeout=10, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA synchronous=FULL")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema(workflow_id)
        self.reconcile()

    def close(self) -> None:
        self.conn.close()

    def __enter__(self) -> "Supervisor":
        return self

    def __exit__(self, *_: object) -> None:
        self.close()
    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Connection]:
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
        except Exception:
            self.conn.rollback()
            raise
        else:
            self.conn.commit()

    def _init_schema(self, workflow_id: str) -> None:
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS supervisor_meta (
            id INTEGER PRIMARY KEY CHECK (id = 1),
            state TEXT NOT NULL,
            workflow_id TEXT NOT NULL,
            current_job_id TEXT,
            authorized_job_id TEXT,
            last_terminal_state TEXT,
            failure_class TEXT,
            failure_message TEXT,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS jobs (
            job_id TEXT PRIMARY KEY,
            workflow_id TEXT NOT NULL,
            status TEXT NOT NULL,
            payload_json TEXT NOT NULL,
            attempt INTEGER NOT NULL DEFAULT 0,
            lease_generation INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            failure_class TEXT,
            failure_message TEXT
        );
        """)
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS worker_leases (
            job_id TEXT PRIMARY KEY,
            holder_id TEXT NOT NULL,
            lease_token TEXT NOT NULL UNIQUE,
            generation INTEGER NOT NULL,
            acquired_at TEXT NOT NULL,
            heartbeat_at TEXT NOT NULL,
            expires_at TEXT NOT NULL,
            FOREIGN KEY(job_id) REFERENCES jobs(job_id)
        );
        CREATE TABLE IF NOT EXISTS runtime_leases (
            name TEXT PRIMARY KEY,
            owner_id TEXT NOT NULL,
            lease_token TEXT NOT NULL UNIQUE,
            heartbeat_at TEXT NOT NULL,
            expires_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            actor TEXT NOT NULL,
            action TEXT NOT NULL,
            from_state TEXT,
            to_state TEXT,
            job_id TEXT,
            details_json TEXT NOT NULL
        );
        """)
        self.conn.executescript("""
        CREATE TABLE IF NOT EXISTS advisories (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ts TEXT NOT NULL,
            job_id TEXT,
            diagnosis TEXT NOT NULL,
            confidence REAL NOT NULL,
            suggested_action TEXT NOT NULL,
            risk TEXT NOT NULL,
            source TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS command_dedupe (
            command_id TEXT PRIMARY KEY,
            command TEXT NOT NULL,
            job_id TEXT,
            actor TEXT NOT NULL,
            created_at TEXT NOT NULL
        );
        """)
        row = self.conn.execute(
            "SELECT workflow_id FROM supervisor_meta WHERE id=1"
        ).fetchone()
        if row is None:
            self.conn.execute(
                """INSERT INTO supervisor_meta
                (id,state,workflow_id,updated_at) VALUES (1,'HOLD',?,?)""",
                (workflow_id, iso()),
            )
            self._audit("system", "INIT", None, "HOLD", None, {"workflow_id": workflow_id})
        elif row["workflow_id"] != workflow_id:
            raise SupervisorError(
                f"database belongs to workflow_id={row['workflow_id']!r}, "
                f"not {workflow_id!r}"
            )
    def _meta(self) -> sqlite3.Row:
        row = self.conn.execute("SELECT * FROM supervisor_meta WHERE id=1").fetchone()
        if row is None:
            raise SupervisorError("missing supervisor_meta")
        return row

    def _job(self, job_id: str) -> sqlite3.Row:
        row = self.conn.execute("SELECT * FROM jobs WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise SupervisorError(f"unknown job_id: {job_id}")
        return row

    def _audit(
        self,
        actor: str,
        action: str,
        from_state: str | None,
        to_state: str | None,
        job_id: str | None,
        details: dict[str, Any] | None = None,
    ) -> None:
        self.conn.execute(
            """INSERT INTO audit_log
            (ts,actor,action,from_state,to_state,job_id,details_json)
            VALUES (?,?,?,?,?,?,?)""",
            (iso(), actor, action, from_state, to_state, job_id,
             json.dumps(details or {}, ensure_ascii=False, sort_keys=True)),
        )
    def _reserve_command(
        self,
        command_id: str | None,
        command: str,
        job_id: str | None,
        actor: str,
    ) -> bool:
        if command_id is None:
            return True
        row = self.conn.execute(
            "SELECT command,job_id FROM command_dedupe WHERE command_id=?",
            (command_id,),
        ).fetchone()
        if row:
            if row["command"] != command or row["job_id"] != job_id:
                raise SupervisorError("command_id conflict: reused with different command")
            return False
        self.conn.execute(
            """INSERT INTO command_dedupe
            (command_id,command,job_id,actor,created_at) VALUES (?,?,?,?,?)""",
            (command_id, command, job_id, actor, iso()),
        )
        return True

    def submit_job(
        self,
        job_id: str,
        payload: dict[str, Any] | None = None,
        actor: str = "operator",
    ) -> dict[str, Any]:
        payload_json = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True)
        with self._tx():
            existing = self.conn.execute(
                "SELECT * FROM jobs WHERE job_id=?", (job_id,)
            ).fetchone()
            if existing:
                if existing["payload_json"] != payload_json:
                    raise SupervisorError("idempotency conflict: same job_id, different payload")
                return dict(existing)
            workflow_id = self._meta()["workflow_id"]
            now = iso()
            self.conn.execute(
                """INSERT INTO jobs
                (job_id,workflow_id,status,payload_json,created_at,updated_at)
                VALUES (?,?,?,?,?,?)""",
                (job_id, workflow_id, "QUEUED", payload_json, now, now),
            )
            self._audit(actor, "SUBMIT", self._meta()["state"],
                        self._meta()["state"], job_id)
            return dict(self._job(job_id))
    def authorize(
        self,
        job_id: str,
        actor: str = "operator",
        command_id: str | None = None,
    ) -> None:
        self.reconcile()
        with self._tx():
            if not self._reserve_command(command_id, "AUTHORIZE", job_id, actor):
                return
            meta = self._meta()
            job = self._job(job_id)
            if meta["state"] != "HOLD":
                raise SupervisorError(f"AUTHORIZE requires HOLD, got {meta['state']}")
            if job["status"] not in {"QUEUED", "PAUSED"}:
                raise SupervisorError(f"job is not authorizable: {job['status']}")
            self.conn.execute(
                "UPDATE jobs SET status='READY',updated_at=? WHERE job_id=?",
                (iso(), job_id),
            )
            self.conn.execute(
                """UPDATE supervisor_meta SET state='AUTHORIZED',
                authorized_job_id=?,current_job_id=?,failure_class=NULL,
                failure_message=NULL,updated_at=? WHERE id=1""",
                (job_id, job_id, iso()),
            )
            self._audit(actor, "AUTHORIZE", "HOLD", "AUTHORIZED", job_id)

    def hold(
        self,
        actor: str = "operator",
        command_id: str | None = None,
    ) -> None:
        self.reconcile()
        with self._tx():
            meta = self._meta()
            job_id = meta["current_job_id"] or meta["authorized_job_id"]
            if not self._reserve_command(command_id, "HOLD", job_id, actor):
                return
            before = meta["state"]
            if before == "HOLD":
                self._audit(actor, "HOLD_NOOP", "HOLD", "HOLD", job_id)
                return
            if job_id:
                job = self._job(job_id)
                if job["status"] in {"READY", "RUNNING", "PAUSED", "FAILED"}:
                    self.conn.execute(
                        "UPDATE jobs SET status='PAUSED',updated_at=? WHERE job_id=?",
                        (iso(), job_id),
                    )
                self.conn.execute("DELETE FROM worker_leases WHERE job_id=?", (job_id,))
            self.conn.execute(
                """UPDATE supervisor_meta SET state='HOLD',
                authorized_job_id=NULL,current_job_id=NULL,
                failure_class=NULL,failure_message=NULL,updated_at=? WHERE id=1""",
                (iso(),),
            )
            self._audit(actor, "HOLD", before, "HOLD", job_id,
                        {"fence": "worker lease revoked"})

    def pause(
        self,
        actor: str = "operator",
        command_id: str | None = None,
    ) -> None:
        self.reconcile()
        with self._tx():
            meta = self._meta()
            job_id = meta["current_job_id"]
            if not self._reserve_command(command_id, "PAUSE", job_id, actor):
                return
            if meta["state"] != "RUNNING":
                raise SupervisorError(f"PAUSE requires RUNNING, got {meta['state']}")
            if not job_id:
                raise SupervisorError("RUNNING without current_job_id")
            self.conn.execute("DELETE FROM worker_leases WHERE job_id=?", (job_id,))
            self.conn.execute(
                "UPDATE jobs SET status='PAUSED',updated_at=? WHERE job_id=?",
                (iso(), job_id),
            )
            self.conn.execute(
                "UPDATE supervisor_meta SET state='PAUSED',updated_at=? WHERE id=1",
                (iso(),),
            )
            self._audit(actor, "PAUSE", "RUNNING", "PAUSED", job_id,
                        {"fence": "worker lease revoked"})

    def resume(
        self,
        actor: str = "operator",
        command_id: str | None = None,
    ) -> None:
        self.reconcile()
        with self._tx():
            meta = self._meta()
            job_id = meta["current_job_id"]
            if not self._reserve_command(command_id, "RESUME", job_id, actor):
                return
            if meta["state"] != "PAUSED":
                raise SupervisorError(f"RESUME requires PAUSED, got {meta['state']}")
            if not job_id:
                raise SupervisorError("PAUSED without current_job_id")
            self.conn.execute(
                "UPDATE jobs SET status='READY',updated_at=? WHERE job_id=?",
                (iso(), job_id),
            )
            self.conn.execute(
                """UPDATE supervisor_meta SET state='AUTHORIZED',
                authorized_job_id=?,updated_at=? WHERE id=1""",
                (job_id, iso()),
            )
            self._audit(actor, "RESUME", "PAUSED", "AUTHORIZED", job_id)

    def cancel(
        self,
        actor: str = "operator",
        command_id: str | None = None,
    ) -> None:
        self.reconcile()
        with self._tx():
            meta = self._meta()
            job_id = meta["current_job_id"] or meta["authorized_job_id"]
            if not self._reserve_command(command_id, "CANCEL", job_id, actor):
                return
            before = meta["state"]
            if before not in {"AUTHORIZED", "RUNNING", "PAUSED", "FAILED"}:
                raise SupervisorError(f"CANCEL not allowed from {before}")
            if not job_id:
                raise SupervisorError("no current job to cancel")
            self.conn.execute("DELETE FROM worker_leases WHERE job_id=?", (job_id,))
            self.conn.execute(
                """UPDATE jobs SET status='CANCELLED',updated_at=?,
                failure_class=NULL,failure_message=NULL WHERE job_id=?""",
                (iso(), job_id),
            )
            self._audit(actor, "CANCEL", before, "CANCELLED", job_id)
            self.conn.execute(
                """UPDATE supervisor_meta SET state='HOLD',current_job_id=NULL,
                authorized_job_id=NULL,last_terminal_state='CANCELLED',
                failure_class=NULL,failure_message=NULL,updated_at=? WHERE id=1""",
                (iso(),),
            )
            self._audit("system", "AUTO_HOLD", "CANCELLED", "HOLD", job_id)

    def retry(
        self,
        actor: str = "operator",
        command_id: str | None = None,
    ) -> None:
        self.reconcile()
        with self._tx():
            meta = self._meta()
            job_id = meta["current_job_id"]
            if not self._reserve_command(command_id, "RETRY", job_id, actor):
                return
            if meta["state"] != "FAILED":
                raise SupervisorError(f"RETRY requires FAILED, got {meta['state']}")
            if not job_id:
                raise SupervisorError("FAILED without current_job_id")
            self.conn.execute(
                """UPDATE jobs SET status='READY',failure_class=NULL,
                failure_message=NULL,updated_at=? WHERE job_id=?""",
                (iso(), job_id),
            )
            self.conn.execute(
                """UPDATE supervisor_meta SET state='AUTHORIZED',
                authorized_job_id=?,failure_class=NULL,failure_message=NULL,
                updated_at=? WHERE id=1""",
                (job_id, iso()),
            )
            self._audit(actor, "RETRY", "FAILED", "AUTHORIZED", job_id)

    def claim(
        self,
        job_id: str,
        holder_id: str,
        lease_seconds: float = 5.0,
    ) -> dict[str, Any]:
        self.reconcile()
        with self._tx():
            meta = self._meta()
            if meta["state"] == "RUNNING" and meta["current_job_id"] == job_id:
                active = self.conn.execute(
                    "SELECT * FROM worker_leases WHERE job_id=?",
                    (job_id,),
                ).fetchone()
                if active and active["holder_id"] == holder_id:
                    return {
                        "job_id": job_id,
                        "lease_token": active["lease_token"],
                        "generation": int(active["generation"]),
                        "expires_at": active["expires_at"],
                        "idempotent": True,
                    }
            if meta["state"] != "AUTHORIZED" or meta["authorized_job_id"] != job_id:
                raise SupervisorError("EXPECTED_GATE: job is not currently authorized")
            job = self._job(job_id)
            if job["status"] != "READY":
                raise SupervisorError(f"job is not READY: {job['status']}")
            active = self.conn.execute("SELECT job_id FROM worker_leases LIMIT 1").fetchone()
            if active:
                raise SupervisorError("worker lease already exists")
            token = str(uuid.uuid4())
            generation = int(job["lease_generation"]) + 1
            now = utcnow()
            expires = now + timedelta(seconds=lease_seconds)
            self.conn.execute(
                """INSERT INTO worker_leases
                (job_id,holder_id,lease_token,generation,acquired_at,heartbeat_at,expires_at)
                VALUES (?,?,?,?,?,?,?)""",
                (job_id, holder_id, token, generation, iso(now), iso(now), iso(expires)),
            )
            self.conn.execute(
                """UPDATE jobs SET status='RUNNING',attempt=attempt+1,
                lease_generation=?,failure_class=NULL,failure_message=NULL,
                updated_at=? WHERE job_id=?""",
                (generation, iso(now), job_id),
            )
            self.conn.execute(
                """UPDATE supervisor_meta SET state='RUNNING',
                authorized_job_id=NULL,current_job_id=?,updated_at=? WHERE id=1""",
                (job_id, iso(now)),
            )
            self._audit(holder_id, "CLAIM", "AUTHORIZED", "RUNNING", job_id,
                        {"generation": generation, "lease_seconds": lease_seconds})
            return {
                "job_id": job_id,
                "lease_token": token,
                "generation": generation,
                "expires_at": iso(expires),
            }

    def heartbeat(
        self,
        job_id: str,
        lease_token: str,
        lease_seconds: float = 5.0,
        lease_generation: int | None = None,
    ) -> str:
        now = utcnow()
        with self._tx():
            lease = self.conn.execute(
                "SELECT * FROM worker_leases WHERE job_id=? AND lease_token=?",
                (job_id, lease_token),
            ).fetchone()
            if lease is None:
                raise SupervisorError("worker is fenced: lease not found")
            if (
                lease_generation is not None
                and int(lease["generation"]) != int(lease_generation)
            ):
                raise SupervisorError("worker is fenced: lease generation mismatch")
            if datetime.fromisoformat(lease["expires_at"]) <= now:
                raise SupervisorError("worker is fenced: lease already expired")
            expires = now + timedelta(seconds=lease_seconds)
            self.conn.execute(
                """UPDATE worker_leases SET heartbeat_at=?,expires_at=?
                WHERE job_id=? AND lease_token=?""",
                (iso(now), iso(expires), job_id, lease_token),
            )
            return iso(expires)

    def worker_permission(
        self,
        job_id: str,
        lease_token: str,
        lease_generation: int | None = None,
    ) -> str:
        self.reconcile()
        meta = self._meta()
        lease = self.conn.execute(
            "SELECT * FROM worker_leases WHERE job_id=? AND lease_token=?",
            (job_id, lease_token),
        ).fetchone()
        generation_ok = (
            lease_generation is None
            or (
                lease is not None
                and int(lease["generation"]) == int(lease_generation)
            )
        )
        if (
            meta["state"] == "RUNNING"
            and meta["current_job_id"] == job_id
            and lease
            and generation_ok
        ):
            return "RUN"
        return "FENCED"

    def record_progress(
        self,
        job_id: str,
        lease_token: str,
        checkpoint: str | None = None,
        metrics: dict[str, Any] | None = None,
        actor: str = "worker",
        lease_generation: int | None = None,
    ) -> None:
        self.reconcile()
        with self._tx():
            meta = self._meta()
            lease = self.conn.execute(
                "SELECT * FROM worker_leases WHERE job_id=? AND lease_token=?",
                (job_id, lease_token),
            ).fetchone()
            if meta["state"] != "RUNNING" or lease is None:
                raise SupervisorError("cannot record progress: worker is fenced")
            if (
                lease_generation is not None
                and int(lease["generation"]) != int(lease_generation)
            ):
                raise SupervisorError(
                    "cannot record progress: lease generation mismatch"
                )
            self._audit(
                actor,
                "PROGRESS",
                "RUNNING",
                "RUNNING",
                job_id,
                {
                    "checkpoint": checkpoint,
                    "metrics": metrics or {},
                    "generation": int(lease["generation"]),
                },
            )

    def complete(
        self,
        job_id: str,
        lease_token: str,
        actor: str = "worker",
        lease_generation: int | None = None,
    ) -> None:
        self.reconcile()
        with self._tx():
            meta = self._meta()
            lease = self.conn.execute(
                "SELECT * FROM worker_leases WHERE job_id=? AND lease_token=?",
                (job_id, lease_token),
            ).fetchone()
            if meta["state"] != "RUNNING" or lease is None:
                raise SupervisorError("cannot complete: worker is fenced")
            if (
                lease_generation is not None
                and int(lease["generation"]) != int(lease_generation)
            ):
                raise SupervisorError("cannot complete: lease generation mismatch")
            self.conn.execute(
                """UPDATE jobs SET status='DONE',updated_at=?,
                failure_class=NULL,failure_message=NULL WHERE job_id=?""",
                (iso(), job_id),
            )
            self.conn.execute("DELETE FROM worker_leases WHERE job_id=?", (job_id,))
            self._audit(actor, "DONE", "RUNNING", "DONE", job_id)
            self.conn.execute(
                """UPDATE supervisor_meta SET state='HOLD',current_job_id=NULL,
                authorized_job_id=NULL,last_terminal_state='DONE',
                failure_class=NULL,failure_message=NULL,updated_at=? WHERE id=1""",
                (iso(),),
            )
            self._audit("system", "AUTO_HOLD", "DONE", "HOLD", job_id)

    def fail(
        self,
        job_id: str,
        lease_token: str,
        failure_class: str,
        message: str,
        actor: str = "worker",
        lease_generation: int | None = None,
    ) -> None:
        if failure_class not in FAILURE_CLASSES:
            raise SupervisorError(f"invalid failure_class: {failure_class}")
        with self._tx():
            meta = self._meta()
            lease = self.conn.execute(
                "SELECT * FROM worker_leases WHERE job_id=? AND lease_token=?",
                (job_id, lease_token),
            ).fetchone()
            if meta["state"] != "RUNNING" or lease is None:
                raise SupervisorError("cannot fail: worker is fenced")
            if (
                lease_generation is not None
                and int(lease["generation"]) != int(lease_generation)
            ):
                raise SupervisorError("cannot fail: lease generation mismatch")
            self.conn.execute("DELETE FROM worker_leases WHERE job_id=?", (job_id,))
            self.conn.execute(
                """UPDATE jobs SET status='FAILED',failure_class=?,
                failure_message=?,updated_at=? WHERE job_id=?""",
                (failure_class, message, iso(), job_id),
            )
            self.conn.execute(
                """UPDATE supervisor_meta SET state='FAILED',current_job_id=?,
                authorized_job_id=NULL,failure_class=?,failure_message=?,
                updated_at=? WHERE id=1""",
                (job_id, failure_class, message, iso()),
            )
            self._audit(actor, "FAIL", "RUNNING", "FAILED", job_id,
                        {"failure_class": failure_class, "message": message})

    def reconcile(self) -> None:
        meta = self._meta()
        if meta["state"] != "RUNNING":
            return
        job_id = meta["current_job_id"]
        if not job_id:
            self._mark_reconcile_failure(None, "RUNNING without current_job_id")
            return
        lease = self.conn.execute(
            "SELECT * FROM worker_leases WHERE job_id=?", (job_id,)
        ).fetchone()
        if lease is None:
            self._mark_reconcile_failure(job_id, "RUNNING without worker lease")
            return
        if datetime.fromisoformat(lease["expires_at"]) <= utcnow():
            self._mark_reconcile_failure(job_id, "worker lease expired")

    def _mark_reconcile_failure(self, job_id: str | None, message: str) -> None:
        with self._tx():
            meta = self._meta()
            if meta["state"] != "RUNNING":
                return
            if job_id:
                self.conn.execute("DELETE FROM worker_leases WHERE job_id=?", (job_id,))
                self.conn.execute(
                    """UPDATE jobs SET status='FAILED',
                    failure_class='KNOWN_RECOVERABLE',failure_message=?,
                    updated_at=? WHERE job_id=?""",
                    (message, iso(), job_id),
                )
            self.conn.execute(
                """UPDATE supervisor_meta SET state='FAILED',
                failure_class='KNOWN_RECOVERABLE',failure_message=?,
                updated_at=? WHERE id=1""",
                (message, iso()),
            )
            self._audit("system", "LEASE_RECOVERY", "RUNNING", "FAILED", job_id,
                        {"failure_class": "KNOWN_RECOVERABLE", "message": message})
    def add_advisory(
        self,
        diagnosis: str,
        confidence: float,
        suggested_action: str,
        risk: str,
        job_id: str | None = None,
        source: str = "ai",
    ) -> int:
        if not 0.0 <= confidence <= 1.0:
            raise SupervisorError("confidence must be between 0 and 1")
        with self._tx():
            cur = self.conn.execute(
                """INSERT INTO advisories
                (ts,job_id,diagnosis,confidence,suggested_action,risk,source)
                VALUES (?,?,?,?,?,?,?)""",
                (iso(), job_id, diagnosis, confidence, suggested_action, risk, source),
            )
            advisory_id = int(cur.lastrowid)
            self._audit(source, "ADVISORY_RECORDED", self._meta()["state"],
                        self._meta()["state"], job_id, {"advisory_id": advisory_id})
            return advisory_id

    def status(self) -> dict[str, Any]:
        self.reconcile()
        meta = dict(self._meta())
        job_id = meta["current_job_id"] or meta["authorized_job_id"]
        job = dict(self._job(job_id)) if job_id else None
        lease = None
        if job_id:
            row = self.conn.execute(
                "SELECT * FROM worker_leases WHERE job_id=?", (job_id,)
            ).fetchone()
            lease = dict(row) if row else None
        return {"supervisor": meta, "job": job, "worker_lease": lease}

    def audit(self, limit: int = 50) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM audit_log ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in reversed(rows)]

    def advisories(self, limit: int = 20) -> list[dict[str, Any]]:
        rows = self.conn.execute(
            "SELECT * FROM advisories ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [dict(r) for r in reversed(rows)]

    def acquire_instance(self, owner_id: str, ttl_seconds: float = 5.0) -> str:
        now = utcnow()
        with self._tx():
            row = self.conn.execute(
                "SELECT * FROM runtime_leases WHERE name='supervisor'"
            ).fetchone()
            if row and datetime.fromisoformat(row["expires_at"]) > now:
                raise SupervisorError(
                    f"supervisor instance already active: owner={row['owner_id']}"
                )
            token = str(uuid.uuid4())
            expires = now + timedelta(seconds=ttl_seconds)
            self.conn.execute(
                """INSERT OR REPLACE INTO runtime_leases
                (name,owner_id,lease_token,heartbeat_at,expires_at)
                VALUES ('supervisor',?,?,?,?)""",
                (owner_id, token, iso(now), iso(expires)),
            )
            self._audit(owner_id, "INSTANCE_ACQUIRE", self._meta()["state"],
                        self._meta()["state"], None,
                        {"ttl_seconds": ttl_seconds})
            return token

    def renew_instance(self, lease_token: str, ttl_seconds: float = 5.0) -> str:
        now = utcnow()
        with self._tx():
            row = self.conn.execute(
                """SELECT * FROM runtime_leases
                WHERE name='supervisor' AND lease_token=?""",
                (lease_token,),
            ).fetchone()
            if row is None:
                raise SupervisorError("runtime instance lease is not owned")
            if datetime.fromisoformat(row["expires_at"]) <= now:
                raise SupervisorError("runtime instance lease already expired")
            expires = now + timedelta(seconds=ttl_seconds)
            self.conn.execute(
                """UPDATE runtime_leases SET heartbeat_at=?,expires_at=?
                WHERE name='supervisor' AND lease_token=?""",
                (iso(now), iso(expires), lease_token),
            )
            return iso(expires)
    def release_instance(self, lease_token: str, actor: str = "runtime") -> None:
        with self._tx():
            row = self.conn.execute(
                """SELECT * FROM runtime_leases
                WHERE name='supervisor' AND lease_token=?""",
                (lease_token,),
            ).fetchone()
            if row is None:
                return
            self.conn.execute(
                "DELETE FROM runtime_leases WHERE name='supervisor' AND lease_token=?",
                (lease_token,),
            )
            self._audit(actor, "INSTANCE_RELEASE", self._meta()["state"],
                        self._meta()["state"], None)
