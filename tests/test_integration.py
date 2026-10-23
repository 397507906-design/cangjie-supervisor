import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
ENV = dict(os.environ, PYTHONPATH=str(ROOT / "src"))


class IntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = str(Path(self.tmp.name) / "integration.db")

    def tearDown(self) -> None:
        self.tmp.cleanup()

    def cli(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "cangjie_supervisor.cli", "--db", self.db, *args],
            cwd=ROOT, env=ENV, text=True, capture_output=True, check=check, timeout=4,
        )

    def state(self) -> dict:
        return json.loads(self.cli("status").stdout)
    def worker(self, job_id: str, *args: str) -> subprocess.CompletedProcess:
        return subprocess.run(
            [sys.executable, "-m", "cangjie_supervisor.mock_worker",
             job_id, "--db", self.db, *args],
            cwd=ROOT, env=ENV, text=True, capture_output=True, timeout=4,
        )

    def wait_state(self, expected: str, timeout: float = 2.0) -> dict:
        end = time.time() + timeout
        last = {}
        while time.time() < end:
            last = self.state()
            if last["supervisor"]["state"] == expected:
                return last
            time.sleep(0.03)
        self.fail(f"state never became {expected}; last={last}")

    def test_mock_worker_done_to_hold_across_processes(self) -> None:
        self.cli("init")
        self.cli("submit", "done-job")
        self.cli("authorize", "done-job")
        result = self.worker("done-job", "--steps", "2", "--interval", "0.02")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        status = self.state()
        self.assertEqual(status["supervisor"]["state"], "HOLD")
        self.assertEqual(status["supervisor"]["last_terminal_state"], "DONE")
    def test_pause_resume_with_live_worker(self) -> None:
        self.cli("init")
        self.cli("submit", "pause-job")
        self.cli("authorize", "pause-job")
        proc = subprocess.Popen(
            [sys.executable, "-m", "cangjie_supervisor.mock_worker",
             "pause-job", "--db", self.db, "--steps", "30",
             "--interval", "0.04", "--lease-seconds", "0.5"],
            cwd=ROOT, env=ENV, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        self.wait_state("RUNNING")
        self.cli("pause")
        out, err = proc.communicate(timeout=3)
        self.assertEqual(proc.returncode, 0, err + out)
        self.assertIn("FENCED", out)
        self.assertEqual(self.state()["supervisor"]["state"], "PAUSED")
        self.cli("resume")
        result = self.worker("pause-job", "--steps", "2", "--interval", "0.02")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(self.state()["supervisor"]["state"], "HOLD")
    def test_abnormal_worker_exit_recovers_from_persistent_lease(self) -> None:
        self.cli("init")
        self.cli("submit", "crash-job")
        self.cli("authorize", "crash-job")
        result = self.worker(
            "crash-job", "--steps", "8", "--interval", "0.02",
            "--lease-seconds", "0.12", "--crash-at", "2",
        )
        self.assertEqual(result.returncode, 37)
        time.sleep(0.16)
        status = self.state()
        self.assertEqual(status["supervisor"]["state"], "FAILED")
        self.assertEqual(status["supervisor"]["failure_class"], "KNOWN_RECOVERABLE")
        self.cli("retry")
        result = self.worker("crash-job", "--steps", "1", "--interval", "0.01")
        self.assertEqual(result.returncode, 0, result.stderr + result.stdout)
        self.assertEqual(self.state()["supervisor"]["state"], "HOLD")

    def test_cli_command_id_retry_is_idempotent(self) -> None:
        self.cli("init")
        self.cli("submit", "cli-cmd-job")
        first = self.cli(
            "authorize", "cli-cmd-job", "--command-id", "cli-authorize-001"
        )
        second = self.cli(
            "authorize", "cli-cmd-job", "--command-id", "cli-authorize-001"
        )
        self.assertEqual(first.returncode, 0)
        self.assertEqual(second.returncode, 0)
        self.assertEqual(self.state()["supervisor"]["state"], "AUTHORIZED")
        audit = json.loads(self.cli("audit").stdout)
        edges = [x for x in audit if x["action"] == "AUTHORIZE"]
        self.assertEqual(len(edges), 1)

    def test_duplicate_runtime_is_rejected_then_stale_lease_expires(self) -> None:
        self.cli("init")
        primary = subprocess.Popen(
            [sys.executable, "-m", "cangjie_supervisor.cli", "--db", self.db,
             "serve", "--owner", "primary", "--ttl", "0.35"],
            cwd=ROOT, env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        try:
            time.sleep(0.12)
            duplicate = self.cli(
                "serve", "--owner", "duplicate", "--ttl", "0.35", check=False
            )
            self.assertEqual(duplicate.returncode, 2)
            self.assertIn("already active", duplicate.stdout)
            primary.terminate()
            primary.wait(timeout=2)
            time.sleep(0.40)
            replacement = subprocess.Popen(
                [sys.executable, "-m", "cangjie_supervisor.cli", "--db", self.db,
                 "serve", "--owner", "replacement", "--ttl", "0.35"],
                cwd=ROOT, env=ENV, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            time.sleep(0.12)
            self.assertIsNone(replacement.poll())
            replacement.terminate()
            replacement.wait(timeout=2)
        finally:
            if primary.poll() is None:
                primary.terminate()
                primary.wait(timeout=2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
