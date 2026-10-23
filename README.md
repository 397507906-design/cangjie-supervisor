# Cangjie Supervisor

A small Python control plane for long-running tasks. It persists job authorization,
worker leases, fencing generations, command IDs and audit events in SQLite.
The core uses the Python standard library and does not call an AI model.

This standalone extraction contains no Cangjie production adapters or operational
data. Licensed under the MIT License; see `LICENSE`.

## What it does

- Starts in HOLD; a worker must claim one explicitly authorized job.
- Revokes a worker's lease on PAUSE, HOLD or CANCEL.
- Rejects expired leases and stale fencing generations.
- Persists state and command deduplication across restarts.
- Records advisory input without granting it control over job state.
- Offers a local CLI, a versioned HTTP worker protocol and mock workers.

## Quick start

Python 3.10 or newer is required. From this directory, run the simulation:

```sh
python examples/local_demo.py
```

The demo uses a temporary database, executes only simulated steps, and checks
that the completed job returns the supervisor to HOLD. It deletes its temporary
directory when finished. No API key, external service or paid model is needed.

To run the existing test suite without installing dependencies:

```sh
# Linux/macOS
PYTHONPATH=src python -B -m unittest discover -s tests -v
```

```powershell
# PowerShell
$env:PYTHONPATH = (Join-Path (Get-Location) 'src')
python -B -m unittest discover -s tests -v
```

For editable installation in a dedicated environment:

```sh
python -m venv .venv
# Activate .venv using your platform's activation command.
python -m pip install -e .
```

The CLI accepts `--db PATH` before the subcommand. Always use a disposable
database when exploring: even `status` opens the database, initializes schema
and reconciles expired leases. It is not a read-only database viewer.

## Boundaries and limitations

The core manages permission; an executor must cooperate with fencing and enforce
the permission check before side effects. Revoking a lease alone cannot stop an
arbitrary external process or undo its work. This is not an exactly-once execution
system. Lease expiry uses wall-clock time, so clock changes matter.

The HTTP service is a development reference using Python's HTTP server and Bearer
tokens. It has no built-in TLS, comprehensive request-size limits or rate limiting.
Use only an isolated trusted test environment; do not expose it directly to the
Internet. Use distinct generated worker/operator tokens and keep them private.
Mock workers are examples, not hardened production executors.

No production adapter, orchestrator, systemd unit, live configuration, desktop UI,
book content, logs or operational databases are included.

## Layout

- `src/cangjie_supervisor/core.py`: persistent state and lease rules
- `protocol.py`, `remote_client.py`, `service.py`: network contract and reference service
- `cli.py`, `mock_worker.py`, `remote_mock_worker.py`: commands and simulations
- `tests/`: core, protocol, loopback service and subprocess integration tests
- `docs/`: architecture and state transitions

See `PROVENANCE.md` for the extraction baseline and `RELEASE_REVIEW.md` for the
remaining publication gates.
