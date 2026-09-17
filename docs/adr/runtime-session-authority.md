# ADR: Runtime-owned session lifecycle

Status: Proposed
Date: 2026-09-17

## Context

A provider can finish a turn while tools remain open. Conversely, gateway request
delivery, permission RPCs, configuration and continuation scheduling can still
block a new prompt after the provider reports idle. Clients cannot reconstruct
session readiness from tools, text, transport leases or provider status alone.

## Decision

The unified runtime projects provider and gateway facts into a versioned session
snapshot. `_openab/session/state` is read-only: it does not resume a session or
claim its output sink. `_openab/runtime/state` enumerates runtime-owned sessions.
A snapshot includes state, phase, label, provider details, operation, unfinished
tools, asynchronous tasks, pending requests, automation, last outcome and action
capabilities. Clients render the projection and forward commands. Only runtime
admission authorizes execution; a read is never a reservation.

Gateway prompt and configuration guards remain visible through response cleanup.
Cancellation fences automated dispatch and keeps a runtime operation outstanding
until native cancellation delivery and prompt completion settle. Pending human
and device requests, including first-wins decisions, live in the runtime and are
scoped to their session and user. They expire after six hours and do not survive
process replacement. Transport/index caches cannot resurrect them.

Background completion continuations run inside OpenAB. Their pending, running,
failed and cancelled lifecycle belongs to the snapshot. Native late output does
not itself reopen a model turn. Provider idle does not terminate a gateway turn
that still owns output. The runtime publishes lifecycle before content so fast
autonomous turns do not lose their first or final output.

Snapshots carry an epoch and monotonic revision. Backend forwarding preserves
all fields. A UI may retain a received copy to render it, but cannot derive or
persist a competing execution state. Observation freshness/disconnection is a
transport property, not a synthesized idle/active transition. Unknown provider
states and flags remain visible and do not grant send capability.

## Compatibility and migration

Snapshot pushes require initialize capability
`clientCapabilities._meta["dev.openab/sessionSnapshots"] = true`.
Runtime continuations require session `_meta["ai.nuphos/runtimeAuthority"] = 2`.
Defaults preserve existing clients' continuation ownership.

Deploy the unified runtime first, then clients that require schemaVersion 2.
Disable backend/client continuation timers when enabling runtime authority.
Migrate pending request handling as a coordinated cutover; existing waits owned
by older backend processes must finish or be explicitly cancelled before that
backend is retired. A runtime restart loses in-memory pending decisions and
continuation scheduling; clients must show the resulting dormant/interrupted
state and require a fresh command instead of replaying old decisions.

Runtime admission can refuse a concurrent command after a successful state read.
The client must surface that refusal and must not cancel the winning command or
replace its session. Durable conversation placement and transcript history remain
application data; they are not execution state.

## Validation

Regression coverage includes idle with an outstanding gateway response,
background tools without a foreground turn, unknown provider status, permission
waits, idempotent/cancelled requests, cancellation during session creation,
continuation deduplication, cancellation fencing and automatic continuation
without a backend-issued prompt. Nuphos additionally verifies replay freshness,
Runtime action capabilities and final buffered output after native idle.
