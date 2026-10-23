# State Machine — extraction candidate v0.2.0

Stable Supervisor states:

HOLD | AUTHORIZED | RUNNING | PAUSED | FAILED

DONE and CANCELLED are terminal transition events. They are written to the Job and audit log, then the Supervisor returns to HOLD.

## Core transitions

- HOLD + AUTHORIZE(job) -> AUTHORIZED
- AUTHORIZED + worker CLAIM -> RUNNING
- AUTHORIZED + HOLD -> HOLD
- RUNNING + PAUSE -> PAUSED
- RUNNING + HOLD -> HOLD
- RUNNING + DONE -> DONE -> HOLD
- RUNNING + FAIL -> FAILED
- RUNNING + expired/missing Worker Lease -> FAILED / KNOWN_RECOVERABLE
- PAUSED + RESUME -> AUTHORIZED
- PAUSED + HOLD -> HOLD
- FAILED + RETRY -> AUTHORIZED
- FAILED + HOLD -> HOLD
- AUTHORIZED/RUNNING/PAUSED/FAILED + CANCEL -> CANCELLED -> HOLD

## Command meaning

AUTHORIZE grants permission to one exact Job ID, not a global run switch.

HOLD is the persistent safe gate. It clears authorization and revokes any active Worker Lease.

PAUSE keeps the Job resumable but fences the currently leased Worker.

RESUME moves a PAUSED Job back to AUTHORIZED. The Worker must claim a new Lease with a new token and generation.

CANCEL marks the Job terminally cancelled, revokes its Lease, records the terminal edge, and returns Supervisor to HOLD.

RETRY is allowed only from FAILED and explicitly returns the same Job to AUTHORIZED.

## Fail-closed rule

Ambiguous execution permission always resolves to no permission. A live OS process without a valid durable Worker Lease is treated as fenced.
