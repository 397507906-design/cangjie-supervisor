"""Run simulated work against a disposable database; no production access."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(__file__).resolve().parents[1]


def main():
    env = dict(os.environ, PYTHONPATH=str(ROOT / "src"), PYTHONDONTWRITEBYTECODE="1")
    with tempfile.TemporaryDirectory(prefix="supervisor-demo-") as directory:
        db = str(Path(directory) / "demo.db")

        def run(module, *args):
            result = subprocess.run(
                [sys.executable, "-B", "-m", "cangjie_supervisor." + module, *args],
                cwd=ROOT, env=env, capture_output=True, text=True, check=True, timeout=10,
            )
            return result.stdout

        run("cli", "--db", db, "init")
        run("cli", "--db", db, "submit", "demo-job")
        run("cli", "--db", db, "authorize", "demo-job")
        run("mock_worker", "demo-job", "--db", db, "--steps", "2", "--interval", "0.02")
        state = json.loads(run("cli", "--db", db, "status"))["supervisor"]
        if state["state"] != "HOLD" or state["last_terminal_state"] != "DONE":
            raise RuntimeError("Unexpected final state")
        print("Simulation passed: authorized -> running -> done -> HOLD")


if __name__ == "__main__":
    main()
