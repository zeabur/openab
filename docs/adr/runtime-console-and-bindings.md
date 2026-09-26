# ADR: Runtime console and per-binding credentials

Status: Proposed
Date: 2026-09-25

## Context

A self-hosted runtime used to have one password. Every application that connected
shared it, the owner had to copy it out of a log or a file, and nothing could take
access away from one application without changing it for all of them. The runtime
also had no place where its owner could see who was connected, sign the provider CLI
in, or check which tools it carries.

## Decision

`OPENAB_RUNTIME_CONSOLE=true` turns on three cooperating parts of the gateway, all on
the same listener as `/acp`. With the flag off, nothing changes.

### Credential store

`/acp` resolves every bearer token through a credential store kept in
`OPENAB_RUNTIME_STATE_DIR/bindings.json`. A binding has its own transport key
(`nrt_<id>_<secret>`) and control key (`nrc_<id>_<secret>`); both are 256-bit and the
store keeps only their SHA-256 digests. A control key is an operator credential for
the methods `OPENAB_ACP_CONTROL_KEY` already gates. A new binding is `pending` until its
first successful authentication and is dropped if unused for 15 minutes, which covers
an exchange whose response never reached the application. Revoking a binding removes
it and closes that binding's live WebSockets with close code `4401`; other bindings
are untouched.

`OPENAB_ACP_AUTH_KEY` / `OPENAB_ACP_CONTROL_KEY` remain valid as deployment principals
that no console action can revoke. A legacy password file is imported once as a
revocable `legacy-password` binding: its transport digest is the password's and its
control digest is `HMAC-SHA256(password, "nuphos-runtime-control-v1")`'s, so an
application that derived its control key from the password keeps working.

### Pairing

The console mints a single-use pairing code: 128 bits as 26 base32 characters, held in
memory for `OPENAB_RUNTIME_PAIRING_TTL_SECS` (10 minutes), at most 5 outstanding. An
application presents it once to `POST /_openab/pairing/exchange` with display metadata
(`backendOrigin`, `teamId`, `teamName`, `pairedBy`, `runtimeRecordId`) and receives
`{bindingId, transportKey, controlKey, runtimeInstanceId, provider, pendingUntil}`. The
code is spent on first presentation whether or not the exchange succeeds. Exchanges
are rate limited (10 per minute overall, 5 per client address).

`GET /_openab/bindings/self` answers `{bindingId, state}` for a binding's key and 401
once it is revoked, so an application can tell "revoked" from "unreachable".
`POST /_openab/bindings/self/revoke` with a control key ends that binding; an
application calls it when it forgets the runtime.

`runtimeInstanceId` is a UUID created once per state directory. It lets an application
recognise the same runtime behind a different URL.

### Session scope

A session belongs to the binding that created it, or to the first binding that resumes a
session nobody owns (one created before this ledger existed, or one whose binding was
revoked). Ownership is kept in `OPENAB_RUNTIME_STATE_DIR/session-owners.json` so a
restart does not hand sessions to whoever resumes first. A binding, with either of its
keys, only sees and acts on its own sessions: `_openab/runtime/state` lists only them,
and `session/resume`, `session/cancel`, `_openab/session/state`, `_openab/session/steer`,
`_openab/session/requests` and the session config methods answer `-32003` for another
binding's session. Revoking a binding releases its sessions. Deployment keys and the
`legacy-password` binding are single-tenant and keep seeing every session.

Each snapshot in `_openab/runtime/state` carries its `sessionId`, so an application can
match it against its own records.

### Console

`GET /` serves a static page (HTML, CSS and JS embedded in the binary) and
`/_openab/console/*` its API.

- **Setup.** An uninitialized runtime (no `console.json`, no legacy password file, no
  deployment key) accepts setup for `OPENAB_RUNTIME_SETUP_WINDOW_SECS` after process
  start. The first request wins: `console.json` is linked into place and a loser gets
  409. The password is chosen (12 characters or more) or generated; only a generated
  one is written to the log, so an owner who lost the page can still find it.
  Afterwards setup is locked until a restart.
- **Login.** argon2id password hash, in-memory sessions (12 h idle, 7 days at most), an
  `HttpOnly; SameSite=Strict` cookie that is `__Host-` prefixed and `Secure` behind
  https. Every write needs a same-host `Origin`/`Referer` and the session's CSRF token.
  Login and password changes are limited to 5 attempts per client address and 30
  overall per minute. Changing the password ends the owner's other sessions and does
  not touch any binding.
- **Pages** are served with `default-src 'self'; frame-ancestors 'none'`,
  `X-Frame-Options: DENY`, `Referrer-Policy: no-referrer` and `Cache-Control: no-store`.
  `GET /` answers 200 in every state and still carries the status rows, so a health
  check that curls it is unaffected.
- **Connect.** Minting a code returns the code, its expiry, the public `/acp` URL and,
  when `OPENAB_RUNTIME_CONNECT_URL_TEMPLATE` is set, a deep link into the application.
- **Bindings** are listed live with their display metadata and can be revoked.
- **Provider sign-in** runs `OPENAB_RUNTIME_LOGIN_COMMAND` through the same single
  runtime-wide login slot as `_openab/runtime/login`, so a console sign-in and one
  started over `/acp` exclude each other and the loser is told the runtime is busy.
  Frames reach the page over SSE with only display fields; a credential a command
  prints never leaves the process.
- **Tools** come from the `OPENAB_RUNTIME_TOOLS_JOB` runtime job's JSON output.

## Consequences

- Bindings no longer see or act on each other's sessions, but they still share one
  runtime: one filesystem, one provider sign-in and account, and runtime-wide jobs and
  sign-in. An agent working for one binding can read and change files another
  binding's agent wrote, and every binding spends the same provider account. Session
  scope is not a sandbox; connect a runtime only to applications that may share it.
- A pairing code handed to the wrong application binds the runtime to it. The code is
  short-lived and single-use, the binding shows up immediately in the console with the
  team and user it names, stays pending until first use, and can be revoked in one
  click.
- A generated console password appears in the process log.
- Pairing codes and sessions are memory-only; a restart invalidates them.
- Resetting a forgotten password means deleting `console.json` (and the legacy key
  file) and restarting. Bindings survive that; deleting `bindings.json` revokes them all.
