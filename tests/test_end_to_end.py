"""Exercise the actual stdio -> authenticated HTTP -> SQLite boundary."""

import contextlib
import json
import os
from pathlib import Path
import queue
import subprocess
import sys
import tempfile
import threading
import unittest

from xordination.server import BrokerServer
from xordination.store import Store


class MCPProcess:
    """An owned MCP subprocess with bounded response and shutdown waits."""

    def __init__(self, endpoint, token, cwd):
        env = dict(os.environ)
        source = str(Path(__file__).resolve().parents[1] / "src")
        env["PYTHONPATH"] = source + os.pathsep + env.get("PYTHONPATH", "")
        env["XORDINATION_TEST_TOKEN"] = token
        self.stderr = tempfile.TemporaryFile(mode="w+t", encoding="utf-8")
        self.process = subprocess.Popen(
            [sys.executable, "-m", "xordination", "stdio", "--endpoint", endpoint,
             "--token-env", "XORDINATION_TEST_TOKEN"],
            stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=self.stderr,
            text=True, encoding="utf-8", bufsize=1, cwd=cwd, env=env,
        )
        self.responses = queue.Queue()
        self.next_id = 0

        def read():
            try:
                for line in self.process.stdout:
                    self.responses.put(line)
            finally:
                self.responses.put(None)

        self.reader = threading.Thread(target=read, daemon=True)
        self.reader.start()

    def __enter__(self):
        try:
            result = self.request("initialize", {
                "protocolVersion": "2025-06-18", "capabilities": {},
                "clientInfo": {"name": "end-to-end-test", "version": "1"},
            })
            if result["protocolVersion"] != "2025-06-18":
                raise AssertionError("MCP frontend did not negotiate the requested supported revision")
            self.notify("notifications/initialized", {})
        except BaseException:
            self.close()
            raise
        return self

    def __exit__(self, *unused):
        self.close()

    def notify(self, method, params):
        self.process.stdin.write(json.dumps({"jsonrpc": "2.0", "method": method, "params": params}) + "\n")
        self.process.stdin.flush()

    def request(self, method, params):
        self.next_id += 1
        ident = self.next_id
        self.process.stdin.write(json.dumps({"jsonrpc": "2.0", "id": ident,
                                            "method": method, "params": params}) + "\n")
        self.process.stdin.flush()
        try:
            line = self.responses.get(timeout=5)
        except queue.Empty:
            raise AssertionError(f"MCP subprocess timed out responding to {method}") from None
        if line is None:
            self.stderr.seek(0)
            raise AssertionError("MCP subprocess closed stdout: " + self.stderr.read(2000))
        packet = json.loads(line)
        if packet.get("jsonrpc") != "2.0" or packet.get("id") != ident:
            raise AssertionError(f"Unexpected MCP response: {packet!r}")
        if "error" in packet:
            raise AssertionError(f"MCP protocol error: {packet['error']!r}")
        return packet["result"]

    def tool(self, name, **arguments):
        result = self.request("tools/call", {"name": "xordination_" + name, "arguments": arguments})
        if result.get("isError"):
            raise AssertionError(f"MCP tool failed: {result!r}")
        structured = result["structuredContent"]
        if json.loads(result["content"][0]["text"]) != structured:
            raise AssertionError("Text and structured MCP results disagree")
        return structured

    def close(self):
        try:
            self.process.stdin.close()
        except (OSError, ValueError):
            pass
        try:
            self.process.wait(timeout=3)
        except subprocess.TimeoutExpired:
            self.process.terminate()
            try:
                self.process.wait(timeout=2)
            except subprocess.TimeoutExpired:
                self.process.kill()
                self.process.wait(timeout=2)
        self.reader.join(timeout=2)
        self.process.stdout.close()
        self.stderr.close()


@contextlib.contextmanager
def broker(path):
    store = Store(path)
    server = None
    thread = None
    try:
        server = BrokerServer(("127.0.0.1", 0), store)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        yield store, "http://127.0.0.1:" + str(server.server_port)
    finally:
        if server:
            server.shutdown()
            server.server_close()
        if thread:
            thread.join(timeout=2)
        store.close()


class EndToEndTests(unittest.TestCase):
    def test_two_stdio_clients_exchange_authenticated_message_and_durable_receipt(self):
        with tempfile.TemporaryDirectory() as temp:
            state = str(Path(temp) / "broker.sqlite3")
            with broker(state) as (store, endpoint):
                store.create_space("independent-project")
                alice = store.join("independent-project", "alice")
                bob = store.join("independent-project", "bob")
                self.assertNotEqual(alice["token"], bob["token"])
                with MCPProcess(endpoint, alice["token"], temp) as sender, MCPProcess(endpoint, bob["token"], temp) as recipient:
                    self.assertNotEqual(sender.process.pid, recipient.process.pid)
                    sent = sender.tool("send", to="bob", body="Question across the real MCP boundary", key="exchange-1")
                    self.assertEqual("alice", sent["sender"])
                    self.assertEqual(alice["generation"], sent["from_generation"])
                    self.assertEqual(bob["generation"], sent["to_generation"])
                    inbox = recipient.tool("inbox", limit=10)
                    self.assertEqual([sent["id"]], [message["id"] for message in inbox["messages"]])
                    self.assertEqual("alice", inbox["messages"][0]["from"])
                    self.assertEqual(sent["body"], inbox["messages"][0]["body"])
                    receipt = recipient.tool("ack", message_id=sent["id"])["receipt"]
                    status = sender.tool("status", message_id=sent["id"])
                    self.assertTrue(status["acknowledged"])
                    self.assertEqual([receipt], status["receipts"])
                    self.assertEqual("bob", receipt["observer"])
                    self.assertEqual(bob["generation"], receipt["generation"])
                    self.assertEqual("client_ack", receipt["kind"])
                    self.assertIsNone(receipt["native_id"])
                    self.assertEqual([], status["attempts"])
            # All processes, the HTTP broker and its original SQLite connection
            # are closed: this assertion cannot pass from transient client state.
            persisted = Store(state)
            try:
                status = persisted.status(persisted.authenticate(alice["token"]), sent["id"])
                self.assertEqual([receipt], status["receipts"])
                self.assertTrue(status["acknowledged"])
                self.assertEqual("alice", status["sender"])
            finally:
                persisted.close()


if __name__ == "__main__":
    unittest.main()
