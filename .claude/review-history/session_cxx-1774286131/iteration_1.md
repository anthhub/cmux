## Codex Review Result

**Score:** 42/100  
**Verdict:** NEEDS WORK

### Summary
This change introduces a new Go IM bridge with a cmux JSON-RPC client, Telegram channel adapter, session/workspace mapping, and a polling-based output watcher. The overall structure is reasonable for an MVP, but core socket API contract mismatches mean the primary IM→cmux→IM flow will fail in practice. There are also concurrency issues around session initialization and shared state that can surface under normal concurrent message delivery.

### Core Path Impact
- **Critical flows affected:** IM message routing to cmux (workspace create, surface create, send/read/focus), output polling and forwarding.
- **Upstream callers at risk:** `channels.Manager` and any registered channels (Telegram now, Slack/Feishu later).
- **Downstream dependencies:** cmux JSON-RPC v2 socket API (`workspace.*`, `surface.*`), `gopkg.in/telebot.v4`.
- **Behavioral correctness:** Not preserved — wrong parameter keys and response parsing break send/read/focus and new session creation.

### Risks
- No reconnect backoff when the socket is unavailable; periodic `ReadText` calls can hammer the socket path at the poll interval (`daemon/im-bridge/bridge/cmux_client.go`, `daemon/im-bridge/bridge/output_watcher.go`).
- Output watchers never stop and poll indefinitely once started; many sessions will accumulate goroutines and polling load (`daemon/im-bridge/bridge/output_watcher.go`, `daemon/im-bridge/bridge/session.go`).

### Issues / Bugs
- Data races on `Session.LastOutput` and `Session.Watching` between the handler and the watcher goroutine; this can cause inconsistent diffs or missed stops (`daemon/im-bridge/bridge/session.go`, `daemon/im-bridge/bridge/output_watcher.go`).
- `bytesReaderImpl.Read` returns `fmt.Errorf("EOF")` instead of `io.EOF`, violating the reader contract and risking failed image uploads (`daemon/im-bridge/channels/telegram.go`).

### Missing Items
- No automated tests for cmux JSON-RPC contract and session/output behavior (`daemon/im-bridge/` has no test files).

### Security Vulnerabilities
- None identified in the reviewed changes.

### Suggestions (only if clearly beneficial and low-risk)
- None.

### Must Fix (Blocking — real bugs or breakage only)
- Use the correct parameter key `surface_id` (not `id`) in `surface.send_text`, `surface.read_text`, and `surface.focus` calls; the current requests will be rejected or ignored by cmux (`daemon/im-bridge/bridge/cmux_client.go`).
- Parse `surface.create` response correctly; cmux returns `surface_id`/`surface_ref`, so unmarshalling into `SurfaceInfo` leaves the ID empty and breaks all subsequent operations (`daemon/im-bridge/bridge/cmux_client.go`).
- Synchronize workspace initialization for a user; concurrent messages can create multiple workspaces and race on agent state (`daemon/im-bridge/bridge/session.go`).