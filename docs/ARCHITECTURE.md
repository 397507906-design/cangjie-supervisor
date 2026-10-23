# Architecture — extraction candidate v0.2.0

## Boundary

The core is executor-agnostic. A supervisor host owns the durable database;
remote worker hosts communicate over the versioned protocol. No shared filesystem
or shared PID namespace is required. The included HTTP transport is a development
reference and is not suitable for direct Internet exposure.

Operator or future desktop client sends commands to the Supervisor Core. The Core contains the rule engine, state machine, authorization gate, Worker Lease manager, runtime instance Lease, failure classifier, and audit writer. SQLite stores the authoritative state on the Supervisor host only. Remote Workers must never open, mount, or otherwise access the Supervisor database.

The AI advisory layer can write observations and suggestions, but it has no state-transition capability.

## Persistence model

SQLite runs in WAL mode with FULL synchronous writes. State-changing operations use immediate transactions.

Persistent tables:

- supervisor_meta: authoritative control-plane state
- jobs: durable Job state, attempts, and fencing generation
- worker_leases: execution permission
- runtime_leases: single Supervisor instance protection
- audit_log: transition and command history
- advisories: diagnosis, confidence, suggested_action, and risk

## Authorization and fencing

Authorization is Job-specific. A successful Worker Lease claim consumes that authorization. Every lease has a random token and a monotonically increasing generation.

PAUSE, HOLD, and CANCEL revoke the active Worker Lease. A process may still physically exist, but without a valid durable Lease it has no permission to continue. Resume therefore requires a newly issued lease.

## Restart semantics

HOLD, AUTHORIZED, PAUSED, and FAILED survive restart exactly as stored. RUNNING is accepted only while its stored Worker Lease remains valid. If a RUNNING state has no valid Lease, reconciliation moves it to FAILED with KNOWN_RECOVERABLE.

No process scan or PID inference is used to reconstruct truth.

## AI advisory layer

The advisory schema is limited to diagnosis, confidence, suggested_action, and risk. Any future AI component must still go through the same human/rule command boundary to cause an action.

## Cross-machine boundary

The Worker boundary is a versioned network protocol. Worker identity, workflow/job identity, Lease token, and fencing generation cross that boundary; PID and local process state do not. An executor must enforce the local deadline and stop side effects when permission expires. The control plane cannot physically stop arbitrary external processes.

The mock workers are test fixtures. SQLite remains on the supervisor host; a
remote worker must use the client protocol rather than opening that database.

## Deferred by design

This extraction excludes all production adapters and the desktop application.
