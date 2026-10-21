"""Exercise the actual stdio -> authenticated HTTP -> SQLite boundary."""

from contextlib import contextmanager
import json
import os
from pathlib import Path
import queue
import signal
import subprocess
import sys
import tempfile
import threading
import unittest

from helpers import MCPProcess, isolated_process_env, owned_broker
from xordination.store import Store


def cli(cwd, *args, token=None):
    """One separately launched client process with its own home and no ambient credentials."""
    with tempfile.TemporaryDirectory(prefix="client-home-", dir=cwd) as home:
        result = subprocess.run([sys.executable, "-m", "xordination", *args], cwd=cwd, timeout=30,
                                env=isolated_process_env(home, token=token), capture_output=True, text=True)
    if result.returncode:
        raise AssertionError("Client process failed: " + result.stderr[-2000:])
    return json.loads(result.stdout)


def call(cwd, endpoint, token, operation, **params):
    return cli(cwd, "call", operation, "--endpoint", endpoint, "--token-env", "XORDINATION_TEST_TOKEN",
               "--params", json.dumps(params), token=token)


@contextmanager
def launched_broker(cwd, state):
    """Start the broker as its own process the way the image does, on loopback port 0."""
    home = tempfile.TemporaryDirectory(prefix="broker-home-", dir=cwd)
    process = subprocess.Popen([sys.executable, "-m", "xordination", "serve", "--state", state,
                                "--host", "127.0.0.1", "--port", "0"], cwd=cwd,
                               env=isolated_process_env(home.name), stdout=subprocess.PIPE,
                               stderr=subprocess.DEVNULL, text=True)
    lines = queue.Queue()
    reader = threading.Thread(target=lambda: lines.put(process.stdout.readline()), daemon=True)
    reader.start()
    try:
        try:
            announced = json.loads(lines.get(timeout=15))
        except (queue.Empty, ValueError):
            raise AssertionError("Separately launched broker did not announce its address") from None
        yield "http://127.0.0.1:" + str(announced["port"])
        # Interrupt lets the broker close its store and save subprocess measurement cleanly.
        process.send_signal(signal.SIGINT if os.name == "posix" else signal.SIGTERM)
        if process.wait(timeout=10) != 0 and os.name == "posix":
            raise AssertionError("Separately launched broker did not stop cleanly")
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        reader.join(timeout=2)
        process.stdout.close()
        home.cleanup()


class EndToEndTests(unittest.TestCase):
    def test_two_stdio_clients_exchange_authenticated_message_and_durable_receipt(self):
        with tempfile.TemporaryDirectory() as temp:
            state = str(Path(temp) / "broker.sqlite3")
            with owned_broker(state) as (store, _, endpoint):
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

    def test_separately_launched_broker_and_isolated_clients_share_one_durable_state(self):
        # AC-14 locally: bootstrap, broker and each client are distinct processes over one state file.
        with tempfile.TemporaryDirectory() as temp:
            state = str(Path(temp) / "state" / "broker.sqlite3")
            cli(temp, "space", "--state", state, "profiles")
            alice = cli(temp, "join", "--state", state, "--space", "profiles", "--participant", "alice")
            bob = cli(temp, "join", "--state", state, "--space", "profiles", "--participant", "bob")
            with launched_broker(temp, state) as endpoint:
                readiness = cli(temp, "readiness", "--endpoint", endpoint, "--token-env",
                                "XORDINATION_TEST_TOKEN", token=alice["token"])["levels"]
                self.assertEqual(readiness["service"]["status"], "passed")
                self.assertEqual(readiness["service"]["participant"]["generation"], alice["generation"])
                self.assertEqual(readiness["communication"]["status"], "unverified")
                sent = call(temp, endpoint, alice["token"], "send", to="bob", body="Across profiles", key="profile-1")
                inbox = call(temp, endpoint, bob["token"], "inbox")["messages"]
                self.assertEqual([sent["id"]], [message["id"] for message in inbox])
                receipt = call(temp, endpoint, bob["token"], "ack", message_id=sent["id"])["receipt"]
            # A replacement broker process on the same state serves the same records.
            with launched_broker(temp, state) as endpoint:
                status = call(temp, endpoint, alice["token"], "status", message_id=sent["id"])
                self.assertTrue(status["acknowledged"])
                self.assertEqual([receipt], status["receipts"])
                repeated = call(temp, endpoint, alice["token"], "send", to="bob", body="Across profiles",
                                key="profile-1")
                self.assertEqual(sent["id"], repeated["id"])


if __name__ == "__main__":
    unittest.main()
