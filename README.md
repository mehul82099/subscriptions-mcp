# subscriptions-mcp

A zero-dependency, single-file MCP stdio server that implements the
**Subscriptions pattern** from MCP protocol version `2026-07-28`:
`subscriptions/listen` long-lived streams, `notifications/subscriptions/acknowledged`,
per-subscription `io.modelcontextprotocol/subscriptionId` correlation,
client cancellation via `notifications/cancelled`, and graceful server-side
closure - over a real, working file watcher.

Subscriptions replaced the former `resources/subscribe` RPC in the 2026-07-28
spec revision. This repo is a reference implementation of the new mechanics on
plain stdio, plus the resource surface (`resources/list`, `resources/read`)
that subscriptions attach to.

## What it does

- Watches a local directory tree (`SUBSCRIPTIONS_MCP_ROOT`, default: cwd).
- Clients open a stream with `subscriptions/listen` and a `notifications`
  filter (`resourceSubscriptions`, `resourcesListChanged`, ...).
- The server acknowledges first with `notifications/subscriptions/acknowledged`,
  echoing only the subset it can honor, then delivers
  `notifications/resources/updated` / `notifications/resources/list_changed`
  on change - every message carrying the subscription's ID in `_meta`.
- Multiple concurrent subscriptions demultiplex by that ID on the shared
  stdio channel.
- `notifications/cancelled` referencing the listen request ID ends the
  subscription and completes the long-lived request. Transport close
  (stdin EOF) gracefully completes all open subscriptions.

## Spec mechanics implemented (2026-07-28)

| Rule | Behavior here |
| --- | --- |
| `subscriptions/listen` opens a long-lived stream | No JSON-RPC response is sent until the subscription closes |
| Server MUST NOT send unrequested notification types | Watcher emits only types present in each subscription's honored filter; `toolsListChanged`/`promptsListChanged` are unsupported (no tools/prompts) and omitted from the ack |
| Acknowledged MUST be first | Registration and the ack write hold the same lock, so no watcher notification can precede it |
| Every stream message carries `io.modelcontextprotocol/subscriptionId` | Set to the JSON-RPC `id` of the originating `subscriptions/listen` request |
| Client cancellation | `notifications/cancelled` with `params.requestId` removes the subscription and completes its request with a `resultType: "complete"` result |
| Graceful closure | On stdin EOF the server responds to every open listen request before exiting, so clients can tell a clean end from a transport drop |
| No state across reconnections | Inherent: the server is a stdio process; a new process means re-subscribing |

Interpretation notes (the spec leaves these open):

- A listen request whose `notifications` filter is invalid (non-object,
  non-boolean flags, non-string URIs) is rejected with `-32602`.
- `resourceSubscriptions` URIs must be `file://` URIs resolving inside the
  watched root; anything else is dropped from the acknowledged filter (the
  client can see the drop by comparing the ack to its request).
- Watching a URI for a file that does not exist yet is allowed; the first
  notification fires when the file is created.
- On client cancellation this server also completes the listen request with
  the graceful-closure result, so cancellation and server-initiated teardown
  look the same to the client.

## Usage

```bash
python3 subscriptions_server.py                      # watch cwd
SUBSCRIPTIONS_MCP_ROOT=/path/to/dir \
SUBSCRIPTIONS_MCP_POLL_MS=100 \
  python3 subscriptions_server.py
```

Point any MCP client that speaks `2026-07-28` subscriptions at it over stdio.

Example session:

```json
→ {"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2026-07-28"}}
← {"jsonrpc":"2.0","id":1,"result":{"resultType":"complete","protocolVersion":"2026-07-28","capabilities":{"resources":{"listChanged":true,"subscribe":true}},"serverInfo":{"name":"subscriptions-mcp","version":"1.0.0"}}}
→ {"jsonrpc":"2.0","id":2,"method":"subscriptions/listen","params":{"notifications":{"resourceSubscriptions":["file:///watched/config.json"]}}}
← {"jsonrpc":"2.0","method":"notifications/subscriptions/acknowledged","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":2},"notifications":{"resourceSubscriptions":["file:///watched/config.json"]}}}
... file changes ...
← {"jsonrpc":"2.0","method":"notifications/resources/updated","params":{"_meta":{"io.modelcontextprotocol/subscriptionId":2},"uri":"file:///watched/config.json"}}
→ {"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":2}}
← {"jsonrpc":"2.0","id":2,"result":{"resultType":"complete","_meta":{"io.modelcontextprotocol/subscriptionId":2}}}
```

## Tests

```bash
python3 -m unittest -v
```

18 stdio integration tests (Python 3.9+, stdlib only) spawn the server as a
subprocess and exercise: capability declaration, ack-first ordering under a
change race, honored-subset acknowledgment, per-ID demultiplexing across
concurrent subscriptions, unrequested-type suppression, create/delete list
changes, cancellation with graceful completion, graceful closure on transport
EOF, and `-32602`/`-32601`/`-32700` error paths. CI runs the suite on
ubuntu-24.04 and windows-latest against Python 3.9 and 3.13.

## Limits (honest)

- Polling watcher (`SUBSCRIPTIONS_MCP_POLL_MS`, default 200 ms), not
  inotify/FSEvents - fine for small trees and as a reference, not tuned for
  huge directories. A change between subscription registration and the first
  poll cycle is baselined, not notified.
- stdio transport only; the HTTP/SSE side of the pattern is out of scope.
- Python 3.9+. No third-party packages.
- The watcher detects content changes by `(mtime_ns, size)`; metadata-only
  changes are not reported.

## Why this exists (gap check)

The Subscriptions pattern is new in protocol version `2026-07-28` (it replaces
the former `resources/subscribe` RPC and the HTTP GET endpoint, per the spec
page). A GitHub repository search for `"subscriptions/listen" mcp`
(api.github.com, November 10, 2026) returned exactly one repository - a
bug-reproduction repro, not a usable server. This repo is a use case for the
new pattern: a minimal, tested, stdio reference implementation you can read in
one sitting.

## Sources

- Subscriptions pattern:
  https://modelcontextprotocol.io/specification/2026-07-28/basic/patterns/subscriptions
- Resources (capability, `subscribe` feature flag, `notifications/resources/updated`):
  https://modelcontextprotocol.io/specification/2026-07-28/server/resources
- MCP specification index: https://modelcontextprotocol.io/llms.txt

## License

MIT (c) 2026 Mehul Jhabak
