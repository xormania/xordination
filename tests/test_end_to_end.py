"""Exercise the actual stdio -> authenticated HTTP -> SQLite boundary."""

from pathlib import Path
import tempfile
import unittest

from helpers import MCPProcess, owned_broker
from xordination.store import Store




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


if __name__ == "__main__":
    unittest.main()
