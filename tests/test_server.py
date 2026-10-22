import concurrent.futures
import io
import json
import socket
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.request import Request, urlopen
from urllib.error import HTTPError
from pathlib import Path
from unittest.mock import patch

from helpers import owned_broker
from xordination.client import Client, BrokerError
from xordination.mcp import _Server
from xordination.server import BrokerServer, Handler


class BrokerIntegration(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        fixture = owned_broker(Path(self.temp.name) / "state.sqlite", max_waits=1)
        self.store, self.server, self.endpoint = fixture.__enter__()
        self.addCleanup(fixture.__exit__, None, None, None)
        self.store.create_space("project")
        a = self.store.join("project", "one")
        b = self.store.join("project", "two")
        self.one = Client(self.endpoint, a["token"], timeout=5)
        self.two = Client(self.endpoint, b["token"], timeout=5)

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
        # Seed directly so the only HTTP send occurs after the empty wait is
        # observed. A future merely being scheduled is not evidence of waiting.
        seed = self.store.send(self.store.authenticate(self.one.token),
                               to="two", body="acknowledge during wait", key="seed")
        cursor = self.two.call("inbox")["cursor"]
        empty_wait = threading.Event()
        original_inbox = self.store.inbox

        def observe_empty(ctx, **params):
            result = original_inbox(ctx, **params)
            if params.get("cursor") == cursor and not result["messages"]:
                empty_wait.set()
            return result

        with patch.object(self.store, "inbox", side_effect=observe_empty), \
                concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(self.two.call, "wait", timeout=3, cursor=cursor)
            try:
                self.assertTrue(empty_wait.wait(1), "HTTP wait never inspected an empty inbox before send")
                self.assertFalse(pending.done())
                receipt = self.two.call("ack", message_id=seed["id"])
                self.assertTrue(receipt["acknowledged"])
                self.assertFalse(pending.done(), "Wait ended before the concurrent acknowledgement")
                self.one.call("send", to="two", body="wake", key="wake-1")
                inbox = pending.result(timeout=2)
            finally:
                self.server.stopping.set()
        self.assertEqual(inbox["messages"][0]["body"], "wake")

    def test_wait_quota_is_released_after_wake_and_store_error(self):
        empty_wait = threading.Event()
        original_inbox = self.store.inbox

        def observe_empty(ctx, **params):
            result = original_inbox(ctx, **params)
            if not result["messages"]:
                empty_wait.set()
            return result

        with patch.object(self.store, "inbox", side_effect=observe_empty), \
                concurrent.futures.ThreadPoolExecutor(max_workers=1) as pool:
            pending = pool.submit(self.two.call, "wait", timeout=3)
            try:
                self.assertTrue(empty_wait.wait(1))
                with self.assertRaises(BrokerError) as caught:
                    self.one.call("wait", timeout=0)
                self.assertEqual("backpressure", caught.exception.code)
                self.assertFalse(pending.done())
                sent = self.one.call("send", to="two", body="release wait slot", key="quota")
                self.assertEqual(sent["id"], pending.result(timeout=2)["messages"][0]["id"])
            finally:
                if not pending.done():
                    self.server.stopping.set()
        self.assertFalse(self.server.stopping.is_set())
        self.assertEqual([], self.one.call("wait", timeout=0)["messages"])
        # Invalid cursor validation fails inside the acquired wait slot.
        with self.assertRaises(BrokerError) as caught:
            self.one.call("wait", timeout=0, cursor="invalid-signature")
        self.assertEqual("invalid_cursor", caught.exception.code)
        self.assertEqual([], self.one.call("wait", timeout=0)["messages"])

    def test_slow_headers_occupy_connection_budget_until_owned_socket_closes(self):
        with owned_broker(Path(self.temp.name) / "connections.sqlite", max_requests=1) as (_, server, endpoint):
            entered, released = threading.Event(), threading.Event()
            setup_count = []
            original_setup = Handler.setup
            original_thread = server.process_request_thread

            def observe_setup(handler):
                original_setup(handler)
                if handler.server is server:
                    setup_count.append(handler.client_address)
                    entered.set()

            def observe_release(*args):
                try:
                    return original_thread(*args)
                finally:
                    released.set()

            with patch.object(Handler, "setup", observe_setup), \
                    patch.object(server, "process_request_thread", side_effect=observe_release):
                with socket.create_connection(server.server_address, timeout=1) as slow:
                    slow.sendall(b"POST /rpc HTTP/1.1\r\nHost: localhost\r\n")
                    self.assertTrue(entered.wait(1), "Slow header connection was never owned")
                    with socket.create_connection(server.server_address, timeout=1) as overflow:
                        try:
                            overflow.sendall(b"GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n")
                            self.assertEqual(b"", overflow.recv(1))
                        except (ConnectionResetError, BrokenPipeError):
                            pass
                    self.assertEqual(1, len(setup_count), "Overflow client bypassed the connection quota")
                    self.assertFalse(released.is_set(), "Incomplete headers did not retain the owned slot")
                    slow.shutdown(socket.SHUT_RDWR)
                self.assertTrue(released.wait(1), "Closed client did not release its connection slot")
                with urlopen(endpoint + "/health", timeout=1) as response:
                    self.assertEqual("xordination", json.load(response)["service"])
                self.assertEqual(2, len(setup_count))

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
        thread = threading.Thread(target=redirect.serve_forever,
                                  kwargs={"poll_interval": 0.02}, daemon=True)
        thread.start()
        try:
            with self.assertRaises(BrokerError):
                Client("http://127.0.0.1:" + str(redirect.server_port), "test-credential").call("peers")
            self.assertEqual(captured, [])
        finally:
            redirect.shutdown()
            redirect.server_close()
            thread.join(timeout=2)

    def test_loopback_client_ignores_ambient_proxies(self):
        # An unreachable ambient proxy without a loopback bypass: a credential sent through it fails.
        proxies = {"http_proxy": "http://127.0.0.1:9", "HTTP_PROXY": "http://127.0.0.1:9",
                   "no_proxy": "", "NO_PROXY": ""}
        with patch.dict("os.environ", proxies):
            client = Client(self.endpoint, self.one.token, timeout=15)
            self.assertEqual(["one", "two"], sorted(row["participant"] for row in client.call("peers")["peers"]))

    def test_cancel_and_native_receipt_operations_are_authenticated_broker_calls(self):
        cancelled = self.one.call("send", to="two", body="no longer needed", key="cancel-1")
        with self.assertRaises(BrokerError) as caught:
            self.two.call("cancel", message_id=cancelled["id"])
        self.assertEqual("forbidden", caught.exception.code)
        self.assertEqual("cancelled", self.one.call("cancel", message_id=cancelled["id"], reason="superseded")["state"])
        self.assertEqual("superseded", self.one.call("status", message_id=cancelled["id"])["cancellation"]["reason"])
        observed = self.one.call("send", to="two", body="observe me", key="observe-1")
        self.two.call("bind", native_id="native-two")
        attempt = self.two.call("claim")
        self.assertEqual(observed["id"], attempt["message"]["id"])
        self.two.call("finish", message_id=observed["id"], attempt_id=attempt["attempt_id"], outcome="submitted")
        receipt = self.two.call("observe", message_id=observed["id"], attempt_id=attempt["attempt_id"],
                                kind="native_insertion", evidence={"native_event": "user_message"})["receipt"]
        self.assertEqual(attempt["attempt_id"], receipt["attempt_id"])
        status = self.one.call("status", message_id=observed["id"])
        self.assertEqual((True, False), (status["received"], status["acknowledged"]))
        # The operator summary is local only; participant credentials cannot reach it.
        with self.assertRaises(BrokerError) as caught:
            self.one.call("space_view", space="project")
        self.assertEqual("invalid_request", caught.exception.code)

    def test_cancelled_body_is_withheld_from_http_and_mcp_inbox_and_wait(self):
        withdrawn = self.one.call("send", to="two", body="withdrawn secret", key="withdrawn")
        self.one.call("cancel", message_id=withdrawn["id"])
        server = _Server(self.two, io.StringIO(), error_output=io.StringIO())
        self.addCleanup(server.close)
        cancel = threading.Event()
        server.dispatch("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                       "clientInfo": {"name": "test", "version": "1"}}, cancel)
        results = [self.two.call("inbox"), self.two.call("wait", timeout=0)]
        results += [server.dispatch("tools/call", {"name": "xordination_" + name, "arguments": arguments}, cancel)["structuredContent"]
                    for name, arguments in (("inbox", {}), ("wait", {"timeout": 0}))]
        for result in results:
            self.assertEqual([], result["messages"])
            self.assertEqual(withdrawn["sequence"], result["position"])
            self.assertNotIn("withdrawn secret", json.dumps(result))
        later = self.one.call("send", to="two", body="later", key="later")
        waited = server.dispatch("tools/call", {"name": "xordination_wait",
                                                "arguments": {"cursor": results[-1]["cursor"], "timeout": 1}}, cancel)
        self.assertEqual([later["id"]], [m["id"] for m in waited["structuredContent"]["messages"]])

    def test_replacement_after_dispatch_authentication_is_fenced_by_store_operation(self):
        sent = self.one.call("send", to="two", body="work", key="queued")
        original = self.store.authenticate
        replacements = []

        def authenticate_then_replace(token, allow_retired=False):
            # The HTTP layer has authenticated the old generation; the participant
            # is replaced before the store operation runs.
            ctx = original(token, allow_retired=allow_retired)
            replacements.append(self.store.join("project", "two", replace=True))
            return ctx

        for operation, params in (("send", {"to": "one", "body": "late", "key": "late"}),
                                  ("claim", {}), ("ack", {"message_id": sent["id"]}),
                                  ("inbox", {})):
            with self.subTest(operation=operation), \
                    patch.object(self.store, "authenticate", side_effect=authenticate_then_replace):
                with self.assertRaises(BrokerError) as caught:
                    self.two.call(operation, **params)
                self.assertEqual("stale_generation", caught.exception.code)
            self.two = Client(self.endpoint, replacements[-1]["token"], timeout=5)
        status = self.one.call("status", message_id=sent["id"])
        self.assertEqual(("queued", [], []), (status["state"], status["attempts"], status["receipts"]))
        self.assertEqual([], self.one.call("inbox")["messages"])

    def test_native_identity_conflict_fences_http_and_mcp_communication(self):
        from xordination.mcp import _Server
        self.two.call("bind", native_id="launch-session", capabilities={"channel_registered": True})
        received = self.one.call("send", to="two", body="Old-generation body", key="received")
        outgoing = self.two.call("send", to="one", body="Before /clear", key="outgoing")
        attempt = self.one.call("claim")["attempt_id"]
        self.one.call("finish", message_id=outgoing["id"], attempt_id=attempt, outcome="uncertain")
        with self.assertRaises(BrokerError) as refused:
            self.two.call("bind", native_id="cleared-session", capabilities={"identity_source": "claude/SessionStart"})
        self.assertEqual("replacement_required", refused.exception.code)
        for operation, params in (("inbox", {}), ("wait", {"timeout": 0}), ("send", {"to": "one", "body": "late", "key": "late"}),
                                  ("ack", {"message_id": received["id"]}), ("retry", {"message_id": outgoing["id"]}),
                                  ("claim", {}), ("recover", {"message_id": received["id"]}), ("peers", {})):
            with self.subTest(transport="http", operation=operation):
                with self.assertRaises(BrokerError) as caught:
                    self.two.call(operation, **params)
                self.assertEqual("replacement_required", caught.exception.code)
        # The model-facing MCP tools reach the same fence through the real HTTP client.
        written = []

        class Output:
            def write(self, wire):
                written.append(wire)

            def flush(self):
                pass

        mcp = _Server(Client(self.endpoint, self.two.token, timeout=5), Output(), error_output=io.StringIO())
        self.addCleanup(mcp.close)
        cancel = threading.Event()
        mcp.dispatch("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                    "clientInfo": {"name": "claude", "version": "fixture"}}, cancel)
        for tool, arguments in (("inbox", {}), ("wait", {"timeout": 0}), ("send", {"to": "one", "body": "late", "key": "late"}),
                                ("ack", {"message_id": received["id"]}), ("retry", {"message_id": outgoing["id"]}), ("peers", {})):
            with self.subTest(transport="mcp", tool=tool):
                result = mcp.dispatch("tools/call", {"name": "xordination_" + tool, "arguments": arguments}, cancel)
                self.assertTrue(result["isError"])
                self.assertEqual("replacement_required", json.loads(result["content"][0]["text"])["error"]["code"])
        diagnostic = mcp.dispatch("tools/call", {"name": "xordination_status", "arguments": {"message_id": received["id"]}}, cancel)
        self.assertFalse(diagnostic["isError"])
        self.assertEqual(("queued", None, "native_identity_conflict"), tuple(diagnostic["structuredContent"][key]
                                                                            for key in ("state", "body", "content_withheld")))
        self.assertNotIn("Old-generation body", json.dumps(diagnostic))
        identity = self.two.call("identity")
        self.assertEqual("cleared-session", identity["native_identity_conflict"]["observed_native_id"])
        status = self.one.call("status", message_id=received["id"])
        self.assertEqual(("queued", []), (status["state"], status["receipts"]))
        self.assertEqual("uncertain", self.one.call("status", message_id=outgoing["id"])["state"])
        self.assertEqual([outgoing["id"]], [message["id"] for message in self.one.call("inbox")["messages"]])

    def test_second_broker_cannot_recover_first_brokers_claim(self):
        sent = self.one.call("send", to="two", body="work", key="lease")
        self.two.call("claim")
        with self.assertRaises(ValueError):
            BrokerServer(("127.0.0.1", 0), self.store)
        self.assertEqual(self.one.call("status", message_id=sent["id"])["state"], "claimed")


if __name__ == "__main__":
    unittest.main()

