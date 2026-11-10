#!/usr/bin/env python3
"""subscriptions-mcp: MCP stdio server implementing the 2026-07-28 Subscriptions
pattern (subscriptions/listen + acknowledged + subscriptionId correlation +
notifications/cancelled + graceful closure) over a local file tree.

Zero dependencies, single file, Python 3.9+.

Environment:
  SUBSCRIPTIONS_MCP_ROOT           watched root (default: current working directory)
  SUBSCRIPTIONS_MCP_POLL_MS        watcher poll interval in ms (default: 200)
  SUBSCRIPTIONS_MCP_MAX_RESOURCES  resources/list cap (default: 1000)
"""

import json
import os
import re
import sys
import threading
import time
from urllib.parse import quote, unquote

LATEST_PROTOCOL_VERSION = "2026-07-28"
PROTOCOL_VERSIONS = ["2026-07-28", "2026-05-12", "2025-11-25", "2025-06-18", "2025-03-26", "2024-11-05"]

ROOT = os.path.realpath(os.environ.get("SUBSCRIPTIONS_MCP_ROOT") or os.getcwd())
POLL_SECONDS = max(0.02, float(os.environ.get("SUBSCRIPTIONS_MCP_POLL_MS", "200")) / 1000.0)
MAX_RESOURCES = int(os.environ.get("SUBSCRIPTIONS_MCP_MAX_RESOURCES", "1000"))

MIME_TYPES = {
    ".txt": "text/plain", ".md": "text/markdown", ".py": "text/x-python",
    ".json": "application/json", ".html": "text/html", ".css": "text/css",
    ".js": "text/javascript", ".csv": "text/csv", ".xml": "application/xml",
    ".yaml": "application/yaml", ".yml": "application/yaml",
}

# All stdout writes (responses and notifications) go through this lock so
# notification emission and request handling can never interleave a line, and
# so the acknowledged-first ordering rule is enforceable.
_write_lock = threading.Lock()
# Active subscriptions: request id -> {"resourceSubscriptions": {uri,...},
#                                       "resourcesListChanged": bool}
_subscriptions = {}
_stop = threading.Event()


def _emit(payload):
    with _write_lock:
        sys.stdout.write(json.dumps(payload) + "\n")
        sys.stdout.flush()


def _error(rid, code, message):
    _emit({"jsonrpc": "2.0", "id": rid, "error": {"code": code, "message": message}})


def _result(rid, result):
    _emit({"jsonrpc": "2.0", "id": rid, "result": result})


# ---------------------------------------------------------------------------
# URI / path mapping (root-contained file:// URIs only)

def uri_to_path(uri):
    """Map a file:// URI to a real path under ROOT, or None if outside/invalid."""
    if not isinstance(uri, str) or not uri.startswith("file:///"):
        return None
    p = unquote(uri[len("file://"):])
    if os.name == "nt":
        if len(p) >= 3 and p[0] == "/" and p[2] == ":":
            p = p[1:]
        p = p.replace("/", os.sep)
    rp = os.path.realpath(p)
    if rp != ROOT and not rp.startswith(ROOT + os.sep):
        return None
    return rp


def path_to_uri(rp):
    p = rp.replace(os.sep, "/")
    if os.name == "nt" and re.match(r"^[A-Za-z]:", p):
        p = "/" + p
    return "file://" + quote(p, safe="/:")


def _stat_signature(path):
    try:
        st = os.stat(path)
    except OSError:
        return None
    if not os.path.isfile(path):
        return None
    return (st.st_mtime_ns, st.st_size)


def _list_files():
    """Relative paths of files under ROOT, capped at MAX_RESOURCES."""
    out = []
    for dirpath, dirnames, filenames in os.walk(ROOT):
        dirnames[:] = [d for d in dirnames if not d.startswith(".")]
        for fn in filenames:
            if fn.startswith("."):
                continue
            rp = os.path.realpath(os.path.join(dirpath, fn))
            if rp == ROOT or not rp.startswith(ROOT + os.sep):
                continue
            out.append(os.path.relpath(rp, ROOT))
            if len(out) >= MAX_RESOURCES:
                return out
    return out


# ---------------------------------------------------------------------------
# Watcher thread

def _snapshot():
    """Current signatures for every watched path, plus the listing if any
    subscription wants listChanged. Caller holds no locks; this function
    takes _write_lock to read the subscription table atomically."""
    with _write_lock:
        subs = {sid: dict(s) for sid, s in _subscriptions.items()}
    paths = {}
    for sub in subs.values():
        for uri in sub["resourceSubscriptions"]:
            rp = uri_to_path(uri)
            if rp is not None:
                paths[rp] = _stat_signature(rp)
    listing = None
    if any(s["resourcesListChanged"] for s in subs.values()):
        listing = tuple(sorted(_list_files()))
    return subs, paths, listing


def _watcher():
    prev_paths = None
    prev_listing = None
    had_listing_watchers = False
    while not _stop.is_set():
        time.sleep(POLL_SECONDS)
        subs, paths, listing = _snapshot()
        if prev_paths is None:
            prev_paths = {}
        for rp, sig in paths.items():
            if rp not in prev_paths:
                continue  # newly watched: baseline only, never emit
            if sig != prev_paths[rp]:
                uri = path_to_uri(rp)
                for sid, sub in subs.items():
                    if uri in sub["resourceSubscriptions"]:
                        _emit({
                            "jsonrpc": "2.0",
                            "method": "notifications/resources/updated",
                            "params": {
                                "_meta": {"io.modelcontextprotocol/subscriptionId": sid},
                                "uri": uri,
                            },
                        })
        watchers_now = any(s["resourcesListChanged"] for s in subs.values())
        if watchers_now:
            if had_listing_watchers and prev_listing is not None and listing != prev_listing:
                for sid, sub in subs.items():
                    if sub["resourcesListChanged"]:
                        _emit({
                            "jsonrpc": "2.0",
                            "method": "notifications/resources/list_changed",
                            "params": {"_meta": {"io.modelcontextprotocol/subscriptionId": sid}},
                        })
            prev_listing = listing
        had_listing_watchers = watchers_now
        prev_paths = paths


# ---------------------------------------------------------------------------
# Subscriptions

def _close_subscription(sid, graceful):
    with _write_lock:
        _subscriptions.pop(sid, None)
    if graceful:
        # Graceful closure: respond to the long-lived subscriptions/listen
        # request so the client can distinguish a clean end from a drop.
        _result(sid, {
            "resultType": "complete",
            "_meta": {"io.modelcontextprotocol/subscriptionId": sid},
        })


def _handle_listen(rid, params):
    if not isinstance(params, dict):
        _error(rid, -32602, "Invalid params: subscriptions/listen requires a params object")
        return
    nf = params.get("notifications", {})
    if not isinstance(nf, dict):
        _error(rid, -32602, "Invalid params: notifications must be an object")
        return
    for key in ("toolsListChanged", "promptsListChanged", "resourcesListChanged"):
        if key in nf and not isinstance(nf[key], bool):
            _error(rid, -32602, "Invalid params: %s must be a boolean" % key)
            return
    rs = nf.get("resourceSubscriptions")
    if rs is not None and (not isinstance(rs, list) or not all(isinstance(u, str) for u in rs)):
        _error(rid, -32602, "Invalid params: resourceSubscriptions must be an array of strings")
        return

    # Honor only what this server can deliver. Tools/prompts do not exist here,
    # so toolsListChanged / promptsListChanged are omitted from the
    # acknowledgment. Resource URIs must resolve inside the watched root.
    honored = {}
    ok_uris = [u for u in (rs or []) if uri_to_path(u) is not None]
    if ok_uris:
        honored["resourceSubscriptions"] = ok_uris
    if nf.get("resourcesListChanged") is True:
        honored["resourcesListChanged"] = True

    # Registration and the acknowledgment share one lock hold, so no watcher
    # notification for this subscription can be emitted before the ack
    # (spec: acknowledged MUST be the first message carrying the ID).
    with _write_lock:
        _subscriptions[rid] = {
            "resourceSubscriptions": set(ok_uris),
            "resourcesListChanged": honored.get("resourcesListChanged", False),
        }
        sys.stdout.write(json.dumps({
            "jsonrpc": "2.0",
            "method": "notifications/subscriptions/acknowledged",
            "params": {
                "_meta": {"io.modelcontextprotocol/subscriptionId": rid},
                "notifications": honored,
            },
        }) + "\n")
        sys.stdout.flush()
    # Long-lived request: no JSON-RPC response until the subscription closes.


def _handle_cancelled(params):
    rid = params.get("requestId") if isinstance(params, dict) else None
    if rid in _subscriptions:
        _close_subscription(rid, graceful=True)


# ---------------------------------------------------------------------------
# Resources

def handle_resources_list(rid):
    resources = []
    for rel in _list_files():
        rp = os.path.realpath(os.path.join(ROOT, rel))
        ext = os.path.splitext(rel)[1].lower()
        resources.append({
            "uri": path_to_uri(rp),
            "name": os.path.basename(rel),
            "mimeType": MIME_TYPES.get(ext, "application/octet-stream"),
        })
    _result(rid, {"resultType": "complete", "resources": resources})


def handle_resources_read(rid, params):
    uri = params.get("uri") if isinstance(params, dict) else None
    rp = uri_to_path(uri)
    if rp is None:
        _error(rid, -32602, "Invalid params: uri must be a file:// URI inside the watched root")
        return
    try:
        with open(rp, "rb") as f:
            data = f.read()
    except OSError:
        _error(rid, -32602, "Invalid params: resource not found: %s" % uri)
        return
    ext = os.path.splitext(rp)[1].lower()
    entry = {"uri": path_to_uri(rp), "mimeType": MIME_TYPES.get(ext, "application/octet-stream")}
    try:
        entry["text"] = data.decode("utf-8")
    except UnicodeDecodeError:
        import base64
        entry["blob"] = base64.b64encode(data).decode("ascii")
    _result(rid, {"resultType": "complete", "contents": [entry]})


# ---------------------------------------------------------------------------
# Dispatch

def handle_request(req):
    rid = req.get("id")
    method = req.get("method")
    params = req.get("params")

    if method == "initialize":
        requested = None
        if isinstance(params, dict):
            requested = params.get("protocolVersion")
        version = requested if requested in PROTOCOL_VERSIONS else LATEST_PROTOCOL_VERSION
        _result(rid, {
            "resultType": "complete",
            "protocolVersion": version,
            "capabilities": {"resources": {"listChanged": True, "subscribe": True}},
            "serverInfo": {"name": "subscriptions-mcp", "version": "1.0.0"},
        })
    elif method == "ping":
        _result(rid, {"resultType": "complete"})
    elif method == "resources/list":
        handle_resources_list(rid)
    elif method == "resources/read":
        handle_resources_read(rid, params)
    elif method == "resources/templates/list":
        _result(rid, {"resultType": "complete", "resourceTemplates": []})
    elif method == "subscriptions/listen":
        _handle_listen(rid, params)
    elif method == "notifications/initialized" or method == "notifications/cancelled":
        if method == "notifications/cancelled":
            _handle_cancelled(params)
    elif rid is None:
        pass  # unknown notification: ignore
    else:
        _error(rid, -32601, "Method not found: %s" % method)


def main():
    t = threading.Thread(target=_watcher, daemon=True)
    t.start()
    try:
        for line in sys.stdin:
            line = line.strip()
            if not line:
                continue
            try:
                req = json.loads(line)
            except json.JSONDecodeError:
                _error(None, -32700, "Parse error")
                continue
            if not isinstance(req, dict) or "method" not in req:
                _error(req.get("id") if isinstance(req, dict) else None, -32600, "Invalid Request")
                continue
            handle_request(req)
    finally:
        _stop.set()
        # Transport close: end every open subscription gracefully so clients
        # see a clean completion response rather than an abrupt drop.
        for sid in list(_subscriptions):
            _close_subscription(sid, graceful=True)


if __name__ == "__main__":
    main()
