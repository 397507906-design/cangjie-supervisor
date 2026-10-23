import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from pathlib import Path

from cangjie_supervisor.protocol import LeaseRef
from cangjie_supervisor.remote_client import (
    RemoteWorkerClient,
    RemoteWorkerError,
    RemoteWorkerNetworkError,
    RemoteWorkerRejected,
)
from cangjie_supervisor.service import ServiceConfig, build_server


class ServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "service.db")
        self.config = ServiceConfig(
            db_path=self.db,
            workflow_id="wf-service-test",
            bind_host="127.0.0.1",
            port=0,
            worker_token="worker-secret",
            operator_token="operator-secret",
            lease_seconds=0.8,
            runtime_ttl_seconds=2.0,
        )
        self.server = build_server(self.config)
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            kwargs={"poll_interval": 0.05},
            daemon=True,
        )
        self.thread.start()
        port = self.server.server_address[1]
        self.base = f"http://127.0.0.1:{port}"
        self.client = RemoteWorkerClient(
            self.base,
            self.config.worker_token,
            "worker-test-01",
            timeout=2.0,
        )

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.tmp.cleanup()
    def operator_request(
        self,
        method: str,
        path: str,
        payload: dict | None = None,
        token: str | None = None,
    ) -> tuple[int, dict | list]:
        data = None if payload is None else json.dumps(payload).encode()
        headers = {
            "Authorization": "Bearer " + (
                token if token is not None else self.config.operator_token
            )
        }
        if data is not None:
            headers["Content-Type"] = "application/json"
        request = urllib.request.Request(
            self.base + path,
            data=data,
            headers=headers,
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=2) as response:
                body = json.loads(response.read().decode())
                return response.status, body
        except urllib.error.HTTPError as exc:
            body = json.loads(exc.read().decode())
            return exc.code, body

    def submit_authorize(self, job_id: str) -> None:
        code, _ = self.operator_request(
            "POST",
            "/v1/operator/submit",
            {"job_id": job_id, "payload": {"kind": "simulation"}},
        )
        self.assertEqual(code, 200)
        code, body = self.operator_request(
            "POST",
            "/v1/operator/command",
            {"command": "AUTHORIZE", "job_id": job_id},
        )
        self.assertEqual(code, 200)
        self.assertEqual(body["supervisor"]["state"], "AUTHORIZED")
    def test_happy_path_uses_core_state_machine(self) -> None:
        health = self.client.health()
        self.assertEqual(health["workflow_id"], "wf-service-test")
        self.client.hello(["simulation"])
        self.submit_authorize("job-happy")

        first = self.client.claim("job-happy")
        second = self.client.claim("job-happy")
        self.assertEqual(first.lease.lease_token, second.lease.lease_token)
        self.assertEqual(first.lease.lease_generation, 1)
        self.assertTrue(second.idempotent)

        self.client.heartbeat(first.lease)
        self.client.progress(
            first.lease,
            checkpoint="step-1",
            metrics={"step": 1},
        )
        result = self.client.complete(first.lease, {"ok": True})
        self.assertEqual(result["state"], "HOLD")

        code, status = self.operator_request("GET", "/v1/operator/status")
        self.assertEqual(code, 200)
        self.assertEqual(status["supervisor"]["state"], "HOLD")
        self.assertEqual(status["supervisor"]["last_terminal_state"], "DONE")

        code, audit = self.operator_request(
            "GET",
            "/v1/operator/audit?limit=50",
        )
        self.assertEqual(code, 200)
        actions = [row["action"] for row in audit]
        self.assertIn("PROGRESS", actions)
        self.assertIn("AUTO_HOLD", actions)
    def test_hello_only_offers_authorized_job(self) -> None:
        before = self.client.hello(["simulation"])
        self.assertIsNone(before["available_job_id"])
        self.submit_authorize("job-offered")
        after = self.client.hello(["simulation"])
        self.assertEqual(after["available_job_id"], "job-offered")

    def test_claim_before_authorize_hits_expected_gate(self) -> None:
        code, _ = self.operator_request(
            "POST",
            "/v1/operator/submit",
            {"job_id": "job-gated"},
        )
        self.assertEqual(code, 200)
        with self.assertRaises(RemoteWorkerError) as ctx:
            self.client.claim("job-gated")
        self.assertIn("EXPECTED_GATE", str(ctx.exception))

    def test_wrong_generation_is_fenced_over_network(self) -> None:
        self.submit_authorize("job-generation")
        claimed = self.client.claim("job-generation")
        lease = claimed.lease
        wrong = LeaseRef(
            workflow_id=lease.workflow_id,
            job_id=lease.job_id,
            lease_token=lease.lease_token,
            lease_generation=lease.lease_generation + 1,
        )
        with self.assertRaises(RemoteWorkerError) as ctx:
            self.client.heartbeat(wrong)
        self.assertIn("generation", str(ctx.exception))

        expires = self.client.heartbeat(lease)
        self.assertTrue(expires)
        self.client.complete(lease)

    def test_remote_client_distinguishes_rejection(self) -> None:
        bad = RemoteWorkerClient(
            self.base,
            "wrong-worker-token",
            "bad-worker",
            timeout=2.0,
        )
        with self.assertRaises(RemoteWorkerRejected) as ctx:
            bad.hello(["simulation"])
        self.assertEqual(ctx.exception.status_code, 401)

    def test_remote_client_distinguishes_network_error(self) -> None:
        offline = RemoteWorkerClient(
            "http://127.0.0.1:1",
            "worker-secret",
            "offline-worker",
            timeout=0.2,
        )
        with self.assertRaises(RemoteWorkerNetworkError):
            offline.health()

    def test_worker_token_cannot_use_operator_endpoint(self) -> None:
        code, body = self.operator_request(
            "GET",
            "/v1/operator/status",
            token=self.config.worker_token,
        )
        self.assertEqual(code, 403)
        self.assertIn("denied", body["error"])
    def test_failure_retry_gets_new_generation(self) -> None:
        self.submit_authorize("job-retry")
        first = self.client.claim("job-retry")
        result = self.client.fail(
            first.lease,
            "KNOWN_RECOVERABLE",
            "simulated remote failure",
        )
        self.assertEqual(result["state"], "FAILED")

        code, status = self.operator_request(
            "POST",
            "/v1/operator/command",
            {"command": "RETRY"},
        )
        self.assertEqual(code, 200)
        self.assertEqual(status["supervisor"]["state"], "AUTHORIZED")

        second = self.client.claim("job-retry")
        self.assertEqual(second.lease.lease_generation, 2)
        self.assertNotEqual(
            first.lease.lease_token,
            second.lease.lease_token,
        )
        with self.assertRaises(RemoteWorkerError):
            self.client.heartbeat(first.lease)
        self.client.complete(second.lease)

        _, final = self.operator_request("GET", "/v1/operator/status")
        self.assertEqual(final["supervisor"]["state"], "HOLD")


if __name__ == "__main__":
    unittest.main(verbosity=2)
