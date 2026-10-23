import json
import tempfile
import time
import unittest
from pathlib import Path

from cangjie_supervisor.core import Supervisor, SupervisorError


class SupervisorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = Path(self.tmp.name) / "supervisor.db"

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def sup(self) -> Supervisor:
        return Supervisor(self.db)

    def prepare(self, s: Supervisor, job_id: str = "job-001") -> str:
        s.submit_job(job_id, {"kind": "simulation"})
        s.authorize(job_id)
        lease = s.claim(job_id, "mock-worker", lease_seconds=1.0)
        return lease["lease_token"]

    def test_initial_hold_and_restart_persistence(self) -> None:
        with self.sup() as s:
            self.assertEqual(s.status()["supervisor"]["state"], "HOLD")
            s.submit_job("job-a")
            s.authorize("job-a")
            self.assertEqual(s.status()["supervisor"]["state"], "AUTHORIZED")
        with self.sup() as s:
            self.assertEqual(s.status()["supervisor"]["state"], "AUTHORIZED")
            self.assertEqual(s.status()["supervisor"]["authorized_job_id"], "job-a")

    def test_submit_is_idempotent(self) -> None:
        with self.sup() as s:
            a = s.submit_job("same", {"x": 1})
            b = s.submit_job("same", {"x": 1})
            self.assertEqual(a["job_id"], b["job_id"])
            with self.assertRaises(SupervisorError):
                s.submit_job("same", {"x": 2})

    def test_single_task_authorization_is_consumed_by_claim(self) -> None:
        with self.sup() as s:
            s.submit_job("one")
            s.submit_job("two")
            s.authorize("one")
            lease = s.claim("one", "worker-a")
            self.assertEqual(s.status()["supervisor"]["state"], "RUNNING")
            with self.assertRaises(SupervisorError):
                s.claim("two", "worker-b")
            self.assertEqual(s.worker_permission("one", lease["lease_token"]), "RUN")
    def test_pause_resume_fences_old_worker(self) -> None:
        with self.sup() as s:
            token1 = self.prepare(s, "pause-job")
            s.pause()
            self.assertEqual(s.status()["supervisor"]["state"], "PAUSED")
            self.assertEqual(s.worker_permission("pause-job", token1), "FENCED")
            s.resume()
            self.assertEqual(s.status()["supervisor"]["state"], "AUTHORIZED")
            lease2 = s.claim("pause-job", "mock-worker-2")
            self.assertNotEqual(token1, lease2["lease_token"])
            self.assertEqual(lease2["generation"], 2)
            s.complete("pause-job", lease2["lease_token"])
            self.assertEqual(s.status()["supervisor"]["state"], "HOLD")

    def test_hold_is_persistent_and_revokes_running_lease(self) -> None:
        with self.sup() as s:
            token = self.prepare(s, "hold-job")
            s.hold()
            self.assertEqual(s.worker_permission("hold-job", token), "FENCED")
            self.assertEqual(s.status()["supervisor"]["state"], "HOLD")
        with self.sup() as s:
            self.assertEqual(s.status()["supervisor"]["state"], "HOLD")
            self.assertIsNone(s.status()["worker_lease"])
    def test_done_transitions_to_hold_and_audits_both_edges(self) -> None:
        with self.sup() as s:
            token = self.prepare(s, "done-job")
            s.complete("done-job", token)
            st = s.status()
            self.assertEqual(st["supervisor"]["state"], "HOLD")
            self.assertEqual(st["supervisor"]["last_terminal_state"], "DONE")
            row = s.conn.execute(
                "SELECT status FROM jobs WHERE job_id='done-job'"
            ).fetchone()
            self.assertEqual(row["status"], "DONE")
            actions = [x["action"] for x in s.audit()]
            self.assertIn("DONE", actions)
            self.assertIn("AUTO_HOLD", actions)

    def test_lease_expiry_becomes_known_recoverable_and_retryable(self) -> None:
        with self.sup() as s:
            s.submit_job("crash-job")
            s.authorize("crash-job")
            s.claim("crash-job", "crashy", lease_seconds=0.10)
        time.sleep(0.16)
        with self.sup() as s:
            st = s.status()
            self.assertEqual(st["supervisor"]["state"], "FAILED")
            self.assertEqual(st["supervisor"]["failure_class"], "KNOWN_RECOVERABLE")
            s.retry()
            self.assertEqual(s.status()["supervisor"]["state"], "AUTHORIZED")
            lease = s.claim("crash-job", "recovery-worker")
            s.complete("crash-job", lease["lease_token"])
            self.assertEqual(s.status()["supervisor"]["state"], "HOLD")

    def test_cancel_returns_to_hold(self) -> None:
        with self.sup() as s:
            token = self.prepare(s, "cancel-job")
            s.cancel()
            self.assertEqual(s.worker_permission("cancel-job", token), "FENCED")
            st = s.status()
            self.assertEqual(st["supervisor"]["state"], "HOLD")
            self.assertEqual(st["supervisor"]["last_terminal_state"], "CANCELLED")

    def test_runtime_single_instance_uses_lease_not_pid(self) -> None:
        with self.sup() as a, self.sup() as b:
            token_a = a.acquire_instance("runtime-a", ttl_seconds=0.10)
            self.assertTrue(token_a)
            with self.assertRaises(SupervisorError):
                b.acquire_instance("runtime-b", ttl_seconds=0.10)
            time.sleep(0.16)
            token_b = b.acquire_instance("runtime-b", ttl_seconds=0.10)
            self.assertTrue(token_b)
            self.assertNotEqual(token_a, token_b)
    def test_ai_advisory_cannot_change_state(self) -> None:
        with self.sup() as s:
            before = s.status()["supervisor"]["state"]
            advisory_id = s.add_advisory(
                diagnosis="simulated observation",
                confidence=0.82,
                suggested_action="operator may inspect logs",
                risk="low",
            )
            after = s.status()["supervisor"]["state"]
            self.assertEqual(before, "HOLD")
            self.assertEqual(after, "HOLD")
            self.assertGreater(advisory_id, 0)
            self.assertEqual(len(s.advisories()), 1)

    def test_duplicate_command_id_is_idempotent(self) -> None:
        with self.sup() as s:
            s.submit_job("cmd-job")
            s.authorize("cmd-job", command_id="cmd-001")
            s.authorize("cmd-job", command_id="cmd-001")
            self.assertEqual(s.status()["supervisor"]["state"], "AUTHORIZED")
            auth_edges = [x for x in s.audit() if x["action"] == "AUTHORIZE"]
            self.assertEqual(len(auth_edges), 1)

    def test_command_id_conflict_rejects_different_target(self) -> None:
        with self.sup() as s:
            s.submit_job("job-a")
            s.submit_job("job-b")
            s.authorize("job-a", command_id="cmd-same")
            with self.assertRaises(SupervisorError):
                s.authorize("job-b", command_id="cmd-same")

    def test_pause_command_retry_is_safe_noop(self) -> None:
        with self.sup() as s:
            self.prepare(s, "pause-command-job")
            s.pause(command_id="pause-001")
            s.pause(command_id="pause-001")
            self.assertEqual(s.status()["supervisor"]["state"], "PAUSED")
            pause_edges = [x for x in s.audit() if x["action"] == "PAUSE"]
            self.assertEqual(len(pause_edges), 1)

    def test_same_worker_claim_retry_returns_same_lease(self) -> None:
        with self.sup() as s:
            s.submit_job("claim-retry")
            s.authorize("claim-retry")
            first = s.claim("claim-retry", "worker-a", lease_seconds=1.0)
            second = s.claim("claim-retry", "worker-a", lease_seconds=1.0)
            self.assertEqual(first["lease_token"], second["lease_token"])
            self.assertEqual(first["generation"], second["generation"])
            self.assertTrue(second["idempotent"])
            job = s.status()["job"]
            self.assertEqual(job["attempt"], 1)

    def test_wrong_generation_is_fenced_even_with_correct_token(self) -> None:
        with self.sup() as s:
            s.submit_job("generation-job")
            s.authorize("generation-job")
            lease = s.claim("generation-job", "worker-a", lease_seconds=1.0)
            token = lease["lease_token"]
            generation = lease["generation"]
            self.assertEqual(
                s.worker_permission("generation-job", token, generation + 1),
                "FENCED",
            )
            with self.assertRaises(SupervisorError):
                s.heartbeat(
                    "generation-job",
                    token,
                    lease_seconds=1.0,
                    lease_generation=generation + 1,
                )
            with self.assertRaises(SupervisorError):
                s.complete(
                    "generation-job",
                    token,
                    lease_generation=generation + 1,
                )
            self.assertEqual(s.status()["supervisor"]["state"], "RUNNING")

    def test_failure_classes_are_constrained(self) -> None:
        with self.sup() as s:
            token = self.prepare(s, "fail-job")
            with self.assertRaises(SupervisorError):
                s.fail("fail-job", token, "MADE_UP", "bad class")
            self.assertEqual(s.status()["supervisor"]["state"], "RUNNING")


if __name__ == "__main__":
    unittest.main(verbosity=2)
