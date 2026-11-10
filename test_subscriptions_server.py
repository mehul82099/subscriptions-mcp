#!/usr/bin/env python3
"""Stdio integration tests for subscriptions-mcp (2026-07-28 Subscriptions pattern).

Spawns the server as a subprocess per test with an isolated watched root and
talks JSON-RPC over pipes. Uses a reader thread + queue so it works on Windows
(select() does not work on pipes there).
"""

import json
import os
import queue
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

HERE = os.path.dirname(os.path.abspath(__file__))
SERVER = os.path.join(HERE, "subscriptions_server.py")
SUB_ID = "io.modelcontextprotocol/subscriptionId"


class Session:
    def __init__(self, root):
        env = dict(os.environ)
        env["SUBSCRIPTIONS_MCP_ROOT"] = root
        env["SUBSCRIPTIONS_MCP_POLL_MS"] = "30"
        self.proc = subprocess.Popen(
            [sys.executable, SERVER],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
            text=True, env=env, cwd=root,
        )
        self.q = queue.Queue()
        self._reader = threading.Thread(target=self._read, daemon=True)
        self._reader.start()

    def _read(self):
        for line in self.proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                self.q.put(json.loads(line))
            except ValueError:
                self.q.put({"_unparsable": line})
        self.q.put(None)  # EOF

    def send(self, msg):
        self.proc.stdin.write(json.dumps(msg) + "\n")
        self.proc.stdin.flush()

    def recv(self, timeout=5.0):
        return self.q.get(timeout=timeout)

    def initialize(self):
        self.send({"jsonrpc": "2.0", "id": "init", "method": "initialize",
                   "params": {"protocolVersion": "2026-07-28"}})
        return self.recv()

    def drain(self, seconds):
        """Collect everything emitted within `seconds`."""
        out = []
        deadline = time.time() + seconds
        while True:
            left = deadline - time.time()
            if left <= 0:
                break
            try:
                m = self.q.get(timeout=left)
            except queue.Empty:
                break
            if m is None:
                break
            out.append(m)
        return out

    def close(self, expect_exit=True):
        try:
            self.proc.stdin.close()
        except Exception:
            pass
        try:
            self.proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        err = self.proc.stderr.read()
        if expect_exit:
            if self.proc.returncode != 0 or err:
                raise AssertionError("server exit=%s stderr=%r" % (self.proc.returncode, err))


class SubscriptionsTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = self.tmp.name
        self.session = Session(self.root)

    def tearDown(self):
        self.session.close(expect_exit=False)
        self.tmp.cleanup()

    def write_file(self, name, content):
        p = os.path.join(self.root, name)
        with open(p, "w", newline="") as f:
            f.write(content)
        return p

    def uri(self, name):
        return Path(os.path.realpath(os.path.join(self.root, name))).as_uri()

    def settle(self, seconds=0.3):
        time.sleep(seconds)

    # -- basics -------------------------------------------------------------

    def test_initialize_declares_subscribe_capability(self):
        r = self.session.initialize()
        self.assertEqual(r["id"], "init")
        self.assertEqual(r["result"]["protocolVersion"], "2026-07-28")
        self.assertEqual(
            r["result"]["capabilities"]["resources"],
            {"listChanged": True, "subscribe": True},
        )

    def test_ping(self):
        self.session.send({"jsonrpc": "2.0", "id": 1, "method": "ping"})
        self.assertEqual(self.session.recv()["result"], {"resultType": "complete"})

    def test_resources_list_and_read_roundtrip(self):
        self.write_file("a.txt", "alpha\n")
        os.mkdir(os.path.join(self.root, "sub"))
        self.write_file(os.path.join("sub", "b.md"), "beta\n")
        self.session.initialize()
        self.session.send({"jsonrpc": "2.0", "id": 2, "method": "resources/list"})
        r = self.session.recv()
        uris = {x["uri"] for x in r["result"]["resources"]}
        self.assertIn(self.uri("a.txt"), uris)
        self.assertIn(self.uri(os.path.join("sub", "b.md")), uris)
        self.session.send({"jsonrpc": "2.0", "id": 3, "method": "resources/read",
                           "params": {"uri": self.uri("a.txt")}})
        r = self.session.recv()
        self.assertEqual(r["result"]["resultType"], "complete")
        contents = r["result"]["contents"]
        self.assertEqual(contents[0]["text"], "alpha\n")
        self.assertEqual(contents[0]["mimeType"], "text/plain")

    def test_read_outside_root_rejected(self):
        self.session.initialize()
        outside = Path(self.root).parent.as_uri() + "/definitely-not-inside-%s.txt" % os.getpid()
        self.session.send({"jsonrpc": "2.0", "id": 4, "method": "resources/read",
                           "params": {"uri": outside}})
        r = self.session.recv()
        self.assertEqual(r["error"]["code"], -32602)

    def test_read_missing_file_rejected(self):
        self.session.initialize()
        self.session.send({"jsonrpc": "2.0", "id": 5, "method": "resources/read",
                           "params": {"uri": self.uri("nope.txt")}})
        r = self.session.recv()
        self.assertEqual(r["error"]["code"], -32602)

    def test_templates_list_empty(self):
        self.session.initialize()
        self.session.send({"jsonrpc": "2.0", "id": 6, "method": "resources/templates/list"})
        r = self.session.recv()
        self.assertEqual(r["result"]["resourceTemplates"], [])

    def test_unknown_method_and_parse_error(self):
        self.session.initialize()
        self.session.send({"jsonrpc": "2.0", "id": 7, "method": "bogus/method"})
        self.assertEqual(self.session.recv()["error"]["code"], -32601)
        self.session.proc.stdin.write("{not json\n")
        self.session.proc.stdin.flush()
        self.assertEqual(self.session.recv()["error"]["code"], -32700)

    # -- subscriptions ------------------------------------------------------

    def test_listen_acknowledged_first_with_subscription_id(self):
        self.session.initialize()
        self.session.send({"jsonrpc": "2.0", "id": 10, "method": "subscriptions/listen",
                           "params": {"notifications": {"resourceSubscriptions": [self.uri("x.txt")]}}})
        ack = self.session.recv()
        self.assertEqual(ack["method"], "notifications/subscriptions/acknowledged")
        self.assertNotIn("id", ack)
        self.assertEqual(ack["params"]["_meta"][SUB_ID], 10)
        self.assertEqual(ack["params"]["notifications"]["resourceSubscriptions"],
                         [self.uri("x.txt")])

    def test_ack_reflects_honored_subset(self):
        self.session.initialize()
        outside = Path(self.root).parent.as_uri() + "/elsewhere.txt"
        self.session.send({"jsonrpc": "2.0", "id": 11, "method": "subscriptions/listen",
                           "params": {"notifications": {
                               "toolsListChanged": True,
                               "promptsListChanged": True,
                               "resourcesListChanged": True,
                               "resourceSubscriptions": [self.uri("ok.txt"), outside],
                           }}})
        ack = self.session.recv()
        nf = ack["params"]["notifications"]
        # No tools/prompts capability: those types are omitted, not echoed.
        self.assertNotIn("toolsListChanged", nf)
        self.assertNotIn("promptsListChanged", nf)
        self.assertTrue(nf["resourcesListChanged"])
        # Outside-root URI dropped from the honored filter.
        self.assertEqual(nf["resourceSubscriptions"], [self.uri("ok.txt")])

    def test_ack_is_first_even_when_file_changes_immediately(self):
        p = self.write_file("fast.txt", "v1")
        self.session.initialize()
        self.session.send({"jsonrpc": "2.0", "id": 12, "method": "subscriptions/listen",
                           "params": {"notifications": {"resourceSubscriptions": [self.uri("fast.txt")]}}})
        self.write_file("fast.txt", "v2 changed")  # race the watcher
        first = self.session.recv()
        self.assertEqual(first["method"], "notifications/subscriptions/acknowledged")
        self.assertEqual(first["params"]["_meta"][SUB_ID], 12)

    def test_resource_update_notification_on_change(self):
        self.write_file("watched.txt", "one\n")
        self.session.initialize()
        self.session.send({"jsonrpc": "2.0", "id": 13, "method": "subscriptions/listen",
                           "params": {"notifications": {"resourceSubscriptions": [self.uri("watched.txt")]}}})
        ack = self.session.recv()
        self.assertEqual(ack["method"], "notifications/subscriptions/acknowledged")
        self.settle()  # let the watcher baseline the current signature
        self.write_file("watched.txt", "two\n")
        note = self.session.recv()
        self.assertEqual(note["method"], "notifications/resources/updated")
        self.assertEqual(note["params"]["_meta"][SUB_ID], 13)
        self.assertEqual(note["params"]["uri"], self.uri("watched.txt"))

    def test_watched_nonexistent_file_fires_on_creation(self):
        self.session.initialize()
        self.session.send({"jsonrpc": "2.0", "id": 14, "method": "subscriptions/listen",
                           "params": {"notifications": {"resourceSubscriptions": [self.uri("later.txt")]}}})
        self.assertEqual(self.session.recv()["method"], "notifications/subscriptions/acknowledged")
        self.settle()
        self.write_file("later.txt", "created\n")
        note = self.session.recv()
        self.assertEqual(note["method"], "notifications/resources/updated")
        self.assertEqual(note["params"]["uri"], self.uri("later.txt"))

    def test_multiple_subscriptions_demux_by_id(self):
        self.write_file("a.txt", "a1\n")
        self.write_file("b.txt", "b1\n")
        self.session.initialize()
        self.session.send({"jsonrpc": "2.0", "id": 20, "method": "subscriptions/listen",
                           "params": {"notifications": {"resourceSubscriptions": [self.uri("a.txt")]}}})
        self.assertEqual(self.session.recv()["params"]["_meta"][SUB_ID], 20)
        self.session.send({"jsonrpc": "2.0", "id": 21, "method": "subscriptions/listen",
                           "params": {"notifications": {"resourceSubscriptions": [self.uri("b.txt")]}}})
        self.assertEqual(self.session.recv()["params"]["_meta"][SUB_ID], 21)
        self.settle()
        self.write_file("a.txt", "a2 changed\n")
        note = self.session.recv()
        self.assertEqual(note["method"], "notifications/resources/updated")
        self.assertEqual(note["params"]["_meta"][SUB_ID], 20)
        self.assertEqual(note["params"]["uri"], self.uri("a.txt"))
        # Nothing for subscription 21 follows.
        for m in self.session.drain(0.5):
            self.assertNotEqual(m.get("params", {}).get("_meta", {}).get(SUB_ID), 21)

    def test_unrequested_notification_types_not_sent(self):
        self.write_file("watched.txt", "x\n")
        self.session.initialize()
        # Subscribe to one file only; do NOT request resourcesListChanged.
        self.session.send({"jsonrpc": "2.0", "id": 22, "method": "subscriptions/listen",
                           "params": {"notifications": {"resourceSubscriptions": [self.uri("watched.txt")]}}})
        self.session.recv()
        self.settle()
        # Creating an unrelated file changes the listing but must not notify.
        self.write_file("unrelated.txt", "y\n")
        time.sleep(0.4)
        self.assertEqual(self.session.drain(0.2), [])

    def test_list_changed_notification_on_create_and_delete(self):
        self.session.initialize()
        self.session.send({"jsonrpc": "2.0", "id": 23, "method": "subscriptions/listen",
                           "params": {"notifications": {"resourcesListChanged": True}}})
        self.assertEqual(self.session.recv()["method"], "notifications/subscriptions/acknowledged")
        self.settle()
        self.write_file("new.txt", "n\n")
        note = self.session.recv()
        self.assertEqual(note["method"], "notifications/resources/list_changed")
        self.assertEqual(note["params"]["_meta"][SUB_ID], 23)
        self.settle(0.2)
        os.remove(os.path.join(self.root, "new.txt"))
        note = self.session.recv()
        self.assertEqual(note["method"], "notifications/resources/list_changed")

    def test_cancel_stops_notifications_and_closes_gracefully(self):
        self.write_file("c.txt", "1\n")
        self.session.initialize()
        self.session.send({"jsonrpc": "2.0", "id": 30, "method": "subscriptions/listen",
                           "params": {"notifications": {"resourceSubscriptions": [self.uri("c.txt")]}}})
        self.session.recv()  # ack
        self.settle()
        self.session.send({"jsonrpc": "2.0", "method": "notifications/cancelled",
                           "params": {"requestId": 30}})
        resp = self.session.recv()
        # Graceful closure: the long-lived listen request gets its response.
        self.assertEqual(resp["id"], 30)
        self.assertEqual(resp["result"]["resultType"], "complete")
        self.assertEqual(resp["result"]["_meta"][SUB_ID], 30)
        self.settle()
        self.write_file("c.txt", "2 changed\n")
        time.sleep(0.4)
        self.assertEqual(self.session.drain(0.2), [])

    def test_graceful_closure_on_stdin_eof(self):
        self.session.initialize()
        self.session.send({"jsonrpc": "2.0", "id": 40, "method": "subscriptions/listen",
                           "params": {"notifications": {"resourcesListChanged": True}}})
        self.session.recv()  # ack
        self.session.proc.stdin.close()
        resp = self.session.recv(timeout=5)
        self.assertEqual(resp["id"], 40)
        self.assertEqual(resp["result"]["resultType"], "complete")
        self.assertEqual(resp["result"]["_meta"][SUB_ID], 40)
        self.assertIsNone(self.session.recv(timeout=5))  # clean EOF
        self.session.proc.wait(timeout=10)
        self.assertEqual(self.session.proc.returncode, 0)
        self.assertEqual(self.session.proc.stderr.read(), "")

    def test_invalid_listen_params(self):
        self.session.initialize()
        bad = [
            (50, None),
            (51, {"notifications": "nope"}),
            (52, {"notifications": {"resourceSubscriptions": "file:///x"}}),
            (53, {"notifications": {"resourceSubscriptions": [123]}}),
            (54, {"notifications": {"resourcesListChanged": "yes"}}),
        ]
        for rid, params in bad:
            msg = {"jsonrpc": "2.0", "id": rid, "method": "subscriptions/listen"}
            if params is not None:
                msg["params"] = params
            self.session.send(msg)
            self.assertEqual(self.session.recv()["error"]["code"], -32602, "id=%r" % rid)


if __name__ == "__main__":
    unittest.main()
