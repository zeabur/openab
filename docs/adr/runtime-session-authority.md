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
not itself reopen a model turn. Native tool calls that remain open when the
provider reports idle are marked as background work by the native reader. Their
terminal events carry the same task-instance provenance as explicit async tasks;
only those background completions may schedule a continuation. Ordinary tools
completed within a foreground turn never schedule another prompt. Explicit async
task correlation takes precedence to avoid two continuations for one command. Provider idle does not terminate a gateway turn
that still owns output. The runtime publishes lifecycle before content so fast
autonomous turns do not lose their first or final output.

Snapshots carry an epoch and monotonic revision. Backend forwarding preserves
all fields. A UI may retain a received copy to render it, but cannot derive or
persist a competing execution state. Observation freshness/disconnection is a
transport property, not a synthesized idle/active transition. Unknown provider
states and flags remain visible and do not grant send capability.

## Operator boundary and accepted events

`OPENAB_ACP_CONTROL_KEY` is a separate, at least 32-character credential, distinct
from `OPENAB_ACP_AUTH_KEY`. Only this operator credential authorizes runtime-wide
inventory, blocking-request RPCs and cross-connection snapshot/cancel operations.
Ordinary ACP authentication never grants this role. The application backend is
the trusted operator: it authenticates end users and delegates their principal in
`userId`; that field alone is not authentication. Protect the operator key as a
backend secret and never distribute it to an untrusted ACP client.

Continuation observation occurs only after reply routing accepts the event.
Accepted prompt origins and observed task-spawn identities fence unknown and
foreign task completions; replacement provider epochs invalidate task ownership.
Native task spawns receive a random ownership token in the provider reader. The
reader stamps subsequent task updates before routing changes between turns and
replaces any subprocess-supplied token. Cross-turn completion requires that same
spawn token; without native provenance, completion must match the spawning
prompt origin. This preserves real background completions during a later turn
without treating membership in the session's past origins as task ownership.

## Operator-driven device sign-in

A runtime an operator started by hand has no pod template, no projected Secret and no
exec channel, so a provider credential minted by an interactive device flow cannot be
seeded from outside it. `_openab/runtime/login` closes that gap under the same operator
boundary as the rest of this ADR: it is refused without `OPENAB_ACP_CONTROL_KEY`, with
the same `-32003` as inventory and the request store.

The method runs `OPENAB_RUNTIME_LOGIN_COMMAND` inside the container and relays each
JSON object the command prints on stdout as an `_openab/runtime/login/frame`
notification, tagged with the caller's `attemptId`. OpenAB does not interpret those
frames and never logs them: they carry a device code, and a provider may put the
credential itself in one. The credential belongs to the container — a command that
installs it locally and reports only that it succeeded keeps it off the wire entirely,
and that is the shape a runtime nobody provisioned should use. Frames are bounded at
128 KiB; anything larger, unparseable, or not a JSON object fails the sign-in rather
than growing a buffer the command controls.

One sign-in runs at a time per runtime, because two device flows would race to write the
same credential file and the loser would silently win; a second request is refused with
`-32005`. `_openab/runtime/login/cancel`, and the close of the connection that started
it, kill the command's whole process group — a provider CLI launches children that hold
the pipes open, so signalling only the parent leaves the sign-in running.

Some providers end their browser flow with a code the user pastes back instead of a
device code the CLI polls for. `_openab/runtime/login/input` with
`{"attemptId": "…", "text": "…"}` writes `text` and a newline to the running command's
stdin and answers `{"delivered": true}`. It sits behind the same `-32003` operator
boundary; `text` must be one line of at most 4 KiB (`-32602` otherwise), and an
`attemptId` that is not the running sign-in, or whose command has already exited, gets
`-32008`. At most a few lines wait for a command that is not reading them; beyond that
the method answers `-32005`. Input is never logged. A command that reads no stdin is
otherwise unaffected.

`_openab/runtime/job` runs one of the jobs the image lists in `OPENAB_RUNTIME_JOBS`,
under the same `-32003` operator boundary. The caller names a job and never sends argv:

```jsonc
// request params
{"jobId": "…", "job": "cost-panel", "stdin": "…", "env": {"K": "V"},
 "timeoutMs": 60000, "maxStdoutBytes": 262144}
// result
{"exitCode": 0, "stdout": "…", "truncated": false, "timedOut": false}
```

The child starts with an empty environment plus `PATH`, `HOME`, `TMPDIR` (a fresh
`0700` directory that is also its cwd and is removed afterwards) and the request's `env`,
which may not set those three, `NODE_OPTIONS`, `BASH_ENV`, `ENV`, `SHELLOPTS` or any
`LD_*`/`DYLD_*` variable.
It runs in its own process group, and the whole group is killed on timeout, on
`_openab/runtime/job/cancel {jobId}`, when the connection that started it closes, and
once stdout exceeds `maxStdoutBytes` (`truncated: true`). `exitCode` is `-1` when the
job ended by signal. stderr is discarded, and nothing a job receives or prints is logged.
`timeoutMs` and `maxStdoutBytes` are clamped to the runtime's own maxima. Jobs are not
limited in number. Errors: `-32007` for a job not in the list, `-32006` when the job cannot start or is cancelled, `-32602` for invalid params or a
`jobId` that is already running.

`_openab/runtime/state` gains `authenticated`. It is `true`/`false` when
`OPENAB_RUNTIME_AUTH_FILE` names the credential the provider CLI reads, and `null` when
it does not: an operator who cannot answer the question must not be told "signed out",
because a container may carry its own credential in a form OpenAB never sees. This lets
a client show that a self-hosted runtime needs signing in before a conversation fails
against it, rather than after.

`_openab/runtime/state` also carries `usage`, the container's own resource figures, so an
operator can chart a runtime it did not provision without a metrics pipeline of its own:
`cpuMillicores` (average since the previous read, from cgroup v2 `cpu.stat`, else
`/proc/stat`; `null` on the first read), `memoryBytes` (working set: `memory.current` minus
`inactive_file`, else `/proc/meminfo`), and `diskUsedBytes` / `diskTotalBytes` across
`OPENAB_RUNTIME_DISK_PATHS`. Any figure OpenAB cannot read is `null`. They sit behind the
same `-32003` operator boundary and are never added to the unauthenticated `/statusz`.

Installing a credential into a process that has already read one has no effect, so a
completed sign-in runs the pool's existing idle sweep. That sweep skips any session
still holding a turn, and a runtime only signs in when it has no working credential, so
there is no live work to strand. No new restart mechanism is introduced.

## Compatibility and migration

A unified runtime advertises `agentCapabilities._meta["dev.openab/sessionAuthority"] = 2`
on initialize so ordinary chat clients can verify support without cross-session
operator queries. Snapshot pushes require initialize capability
`clientCapabilities._meta["dev.openab/sessionSnapshots"] = true`.
Runtime continuations require session `_meta["ai.nuphos/runtimeAuthority"] = 2`.
Defaults preserve existing clients' continuation ownership.
A prompt can separately opt in with `_meta["ai.nuphos/acknowledgePrompt"] = true`.
After admission and before provider dispatch, the runtime emits notification
`_openab/session/prompt_accepted` with `{sessionId, requestId}`. Clients use this
acknowledgement to bind command-local output and principal context only to the
accepted command. A refused prompt receives its JSON-RPC error without this
notification. Clients without the opt-in retain the standard ACP response flow.

Provision the separate operator key before migrating backend control queries.
Ordinary clients may read v2 snapshots only for sessions attached to their own
connection; inventory and request-store RPCs require the operator credential.

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
