import concurrent.futures
import json
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from pathlib import Path

from xordination.client import Client, BrokerError
from xordination.server import BrokerServer
from xordination.store import Store


class BrokerIntegration(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.store = Store(str(Path(self.temp.name) / "state.sqlite"))
        self.store.create_space("project")
        a = self.store.join("project", "one")
        b = self.store.join("project", "two")
        self.server = BrokerServer(("127.0.0.1", 0), self.store, max_waits=1)
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()
        self.endpoint = "http://127.0.0.1:" + str(self.server.server_port)
        self.one = Client(self.endpoint, a["token"])
        self.two = Client(self.endpoint, b["token"])

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        self.store.close()
        self.temp.cleanup()

    def test_native_boundary_and_authenticated_exchange(self):
        with urlopen(self.endpoint + "/health") as response:
            health = json.load(response)
        self.assertFalse(health["native_communication_verified"])
        with self.assertRaises(BrokerError):
            Client(self.endpoint, "invalid").call("peers")
        sent = self.one.call("send", to="two", body="Question", key="request-1")
        inbox = self.two.call("inbox")
        self.assertEqual(len(inbox["messages"]), 1)
        message_id = inbox["messages"][0]["id"]
        self.assertEqual(sent["id"], message_id)
        self.two.call("ack", message_id=message_id)
        self.assertEqual(self.two.call("inbox")["messages"][0]["id"], message_id)

    def test_long_wait_does_not_block_send_or_ack(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            pending = pool.submit(self.two.call, "wait", timeout=2)
            self.one.call("send", to="two", body="wake", key="wake-1")
            inbox = pending.result(timeout=3)
        self.assertEqual(inbox["messages"][0]["body"], "wake")

    def test_rejects_browser_origin_and_payload_without_exposing_identity(self):
        request = Request(self.endpoint + "/rpc",
                          data=json.dumps({"operation": "peers", "params": {}}).encode(),
                          headers={"Authorization": "Bearer " + self.one.token,
                                   "Origin": "https://example.invalid",
                                   "Content-Type": "application/json"})
        with self.assertRaises(HTTPError) as caught:
            urlopen(request)
        self.assertEqual(caught.exception.code, 403)
        error = json.load(caught.exception)
        self.assertNotIn(self.one.token, json.dumps(error))
        with self.assertRaises(BrokerError):
            self.one.call("send", to="two", body="x", key="x", sender="two")

    def test_credential_does_not_follow_broker_redirect(self):
        captured = []
        class Redirect(BaseHTTPRequestHandler):
            def do_POST(handler):
                handler.send_response(302)
                handler.send_header("Location", "/capture")
                handler.end_headers()
            def do_GET(handler):
                captured.append(handler.headers.get("Authorization"))
                handler.send_response(200)
                handler.end_headers()
            def log_message(handler, *args):
                pass
        redirect = ThreadingHTTPServer(("127.0.0.1", 0), Redirect)
        thread = threading.Thread(target=redirect.serve_forever, daemon=True)
        thread.start()
        try:
            with self.assertRaises(BrokerError):
                Client("http://127.0.0.1:" + str(redirect.server_port), "test-credential").call("peers")
            self.assertEqual(captured, [])
        finally:
            redirect.shutdown()
            redirect.server_close()
            thread.join(timeout=2)

    def test_second_broker_cannot_recover_first_brokers_claim(self):
        sent = self.one.call("send", to="two", body="work", key="lease")
        self.two.call("claim")
        with self.assertRaises(ValueError):
            BrokerServer(("127.0.0.1", 0), self.store)
        self.assertEqual(self.one.call("status", message_id=sent["id"])["state"], "claimed")


if __name__ == "__main__":
    unittest.main()

