import io
import json
import threading
import time
import unittest
from unittest.mock import patch

from xordination.client import BrokerError
from xordination.mcp import INSTRUCTIONS, MAX_LINE, TOOLS, ProtocolError, _Server, run_stdio


INITIALIZE = {"protocolVersion": "2025-06-18", "capabilities": {},
              "clientInfo": {"name": "test-client", "version": "1"}}


class Output(io.StringIO):
    def __init__(self):
        super().__init__()
        self.changed = threading.Condition()

    def write(self, text):
        with self.changed:
            result = super().write(text)
            self.changed.notify_all()
            return result

    def packets(self):
        with self.changed:
            return [json.loads(line) for line in self.getvalue().splitlines()]

    def await_packet(self, predicate, timeout=2):
        deadline = time.monotonic() + timeout
        with self.changed:
            while True:
                for line in self.getvalue().splitlines():
                    packet = json.loads(line)
                    if predicate(packet):
                        return packet
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    raise AssertionError("Timed out waiting for MCP output")
                self.changed.wait(remaining)


class FakeClient:
    def __init__(self):
        self.calls = []
        self.handler = None

    def call(self, operation, **params):
        self.calls.append((operation, params))
        if self.handler:
            return self.handler(operation, params)
        return {"operation": operation, "params": params}


class MCPTests(unittest.TestCase):
    def setUp(self):
        self.client = FakeClient()
        self.output = Output()
        self.server = _Server(self.client, self.output, error_output=io.StringIO())
        self.addCleanup(self.server.close)
        self.cancel = threading.Event()

    def initialize(self, version="2025-06-18"):
        return self.server.dispatch("initialize", {**INITIALIZE, "protocolVersion": version}, self.cancel)

    def call(self, name, arguments=None):
        return self.server.dispatch("tools/call", {"name": name, "arguments": arguments or {}}, self.cancel)

    def test_negotiation_and_small_tool_contract(self):
        result = self.initialize("2026-07-28")
        self.assertEqual("2025-11-25", result["protocolVersion"])
        self.assertNotIn("experimental", result["capabilities"])
        self.assertEqual(INSTRUCTIONS, result["instructions"])
        names = [tool["name"] for tool in self.server.dispatch("tools/list", {}, self.cancel)["tools"]]
        self.assertEqual(["xordination_" + name for name in ("peers", "send", "inbox", "wait", "ack", "status", "retry")], names)
        self.assertEqual(7, len(TOOLS))
        with self.assertRaises(ProtocolError):
            self.initialize()

    def test_send_and_reply_preserve_exact_arguments_and_structured_result(self):
        self.initialize()
        args = {"to": "peer-b", "body": "Observed result", "key": "idempotency-key",
                "reply_to": "message-a", "to_generation": "generation-b",
                "metadata": {"task": "opaque-value"}, "expires_in": 30}
        result = self.call("xordination_send", args)
        self.assertEqual(("send", args), self.client.calls[-1])
        self.assertFalse(result["isError"])
        self.assertEqual(result["structuredContent"], json.loads(result["content"][0]["text"]))
        self.call("xordination_inbox", {"cursor": "signed.cursor", "limit": 10, "unacknowledged": True})
        self.assertEqual("signed.cursor", self.client.calls[-1][1]["cursor"])
        self.call("xordination_status", {"message_id": "message-a", "limit": 5})
        self.assertEqual(("status", {"message_id": "message-a", "limit": 5}), self.client.calls[-1])

    def test_schema_errors_never_reach_broker(self):
        self.initialize()
        invalid = [
            ("xordination_send", {"to": "b", "body": "hello", "key": "k", "sender": "spoofed"}),
            ("xordination_send", {"to": "b", "body": "hello"}),
            ("xordination_send", {"to": "b", "body": "hello", "key": "k", "expires_in": 0}),
            ("xordination_inbox", {"cursor": 1}),
            ("xordination_inbox", {"limit": True}),
            ("xordination_inbox", {"limit": 10 ** 400}),
            ("xordination_wait", {"timeout": 31}),
            ("xordination_wait", {"timeout": float("nan")}),
            ("xordination_ack", {"message_id": ""}),
            ("xordination_status", {"message_id": "m", "limit": 0}),
            ("xordination_status", {"message_id": "m", "limit": 101}),
            ("xordination_missing", {}),
        ]
        for name, args in invalid:
            with self.subTest(name=name, args=args), self.assertRaises(ProtocolError) as caught:
                self.call(name, args)
            self.assertEqual(-32602, caught.exception.code)
        self.assertEqual([], self.client.calls)

    def test_json_rpc_invalid_envelopes_and_unknown_methods(self):
        self.server.receive([])
        self.server.receive({"jsonrpc": "2.0", "method": "ping", "id": True})
        self.server.receive({"jsonrpc": "2.0", "method": "ping", "id": "bad-params", "params": []})
        self.assertEqual([-32600, -32600, -32602], [p["error"]["code"] for p in self.output.packets()])
        self.initialize()
        self.server.receive({"jsonrpc": "2.0", "id": "unknown", "method": "bogus"})
        packet = self.output.await_packet(lambda p: p.get("id") == "unknown")
        self.assertEqual(-32601, packet["error"]["code"])
        count = len(self.output.packets())
        self.server.receive({"jsonrpc": "2.0", "method": "notifications/unknown"})
        self.assertEqual(count, len(self.output.packets()))

    def test_wait_saturation_does_not_starve_send_and_cancellation_ends_waits(self):
        self.initialize()
        entered = threading.Condition()
        active_requests = set()
        active_workers = set()
        release = threading.Event()
        self.addCleanup(release.set)

        def handler(operation, params):
            if operation == "wait":
                with entered:
                    active_requests.add(params["cursor"])
                    active_workers.add(threading.get_ident())
                    entered.notify_all()
                if not release.wait(5):
                    raise TimeoutError("Active wait requests were not released")
                return {"messages": [], "cursor": "position-0"}
            return {"id": "accepted", "state": "queued"}

        self.client.handler = handler
        for ident in range(4):
            self.server.receive({"jsonrpc": "2.0", "id": ident, "method": "tools/call",
                                 "params": {"name": "xordination_wait", "arguments": {
                                     "cursor": "request-" + str(ident), "timeout": 30}}})
        with entered:
            self.assertTrue(entered.wait_for(lambda: len(active_requests) == 4, timeout=2))
            self.assertEqual({"request-" + str(ident) for ident in range(4)}, active_requests)
            self.assertEqual(4, len(active_workers))
        self.server.receive({"jsonrpc": "2.0", "id": "overload", "method": "tools/call",
                             "params": {"name": "xordination_wait", "arguments": {"timeout": 30}}})
        self.assertEqual(-32000, self.output.await_packet(lambda p: p.get("id") == "overload")["error"]["code"])
        self.server.receive({"jsonrpc": "2.0", "id": "send", "method": "tools/call",
                             "params": {"name": "xordination_send", "arguments": {"to": "b", "body": "hello", "key": "k"}}})
        self.assertFalse(self.output.await_packet(lambda p: p.get("id") == "send")["result"]["isError"])
        for ident in range(4):
            self.server.receive({"jsonrpc": "2.0", "method": "notifications/cancelled", "params": {"requestId": ident}})
        release.set()
        for ident in range(4):
            self.assertEqual(-32800, self.output.await_packet(lambda p: p.get("id") == ident)["error"]["code"])  # noqa: B023

    def test_wait_returns_recovery_cursor_without_repeating_stale_cursor(self):
        self.initialize()
        self.client.handler = lambda op, params: {"messages": [], "resync_required": True, "cursor": "new-epoch"}
        result = self.call("xordination_wait", {"cursor": "old-epoch", "timeout": 30})
        self.assertTrue(result["structuredContent"]["resync_required"])
        self.assertEqual(1, len(self.client.calls))

    def test_broker_rejections_are_tool_errors_without_exposing_network_details(self):
        self.initialize()

        def reject(operation, params):
            raise BrokerError("idempotency_conflict", "Key already has a different payload")

        self.client.handler = reject
        result = self.call("xordination_send", {"to": "b", "body": "hello", "key": "k"})
        self.assertTrue(result["isError"])
        self.assertIn("idempotency_conflict", result["content"][0]["text"])

        def network_failure(operation, params):
            raise BrokerError("connection_error", "private-network-detail")

        self.client.handler = network_failure
        result = self.call("xordination_peers")
        self.assertNotIn("private-network-detail", result["content"][0]["text"])

    def test_channel_waits_for_registration_and_reports_submission_not_receipt(self):
        self.server.channel = True
        self.assertEqual({"claude/channel": {}}, self.initialize()["capabilities"]["experimental"])
        registered = threading.Event()
        checked = threading.Event()
        finished = threading.Event()
        messages = [{"id": "message-1", "from": "peer-b", "body": "Question", "to_generation": "generation-a"}]

        def handler(operation, params):
            if operation == "identity":
                checked.set()
                return {"capabilities": {"channel_registered": registered.is_set()}}
            if operation == "claim":
                return {"message": messages.pop() if messages else None, "attempt_id": "attempt-1"}
            if operation == "finish":
                finished.set()
                return {"state": params["outcome"]}
            raise AssertionError(operation)

        self.client.handler = handler
        self.server.receive({"jsonrpc": "2.0", "method": "notifications/initialized"})
        self.assertTrue(checked.wait(2))
        self.assertFalse(any(op == "claim" for op, _ in self.client.calls))
        self.assertEqual([], self.output.packets())
        registered.set()
        self.assertTrue(finished.wait(2))
        packet = self.output.await_packet(lambda p: p.get("method") == "notifications/claude/channel")
        self.assertIn("peer-b", packet["params"]["content"])
        self.assertEqual("message-1", packet["params"]["meta"]["message_id"])
        finish = next(params for operation, params in self.client.calls if operation == "finish")
        self.assertEqual("submitted", finish["outcome"])
        self.assertFalse(any(op == "ack" for op, _ in self.client.calls))

    def test_channel_failed_write_is_uncertain_and_not_automatically_replayed(self):
        self.server.channel = True
        self.initialize()
        done = threading.Event()

        class BrokenOutput:
            def write(self, text):
                raise BrokenPipeError("recipient disconnected")

        self.server.output = BrokenOutput()

        def handler(operation, params):
            if operation == "identity":
                return {"capabilities": {"channel_registered": True}}
            if operation == "claim":
                return {"message": {"id": "m", "from": "b", "body": "hello"}, "attempt_id": "a"}
            if operation == "finish":
                done.set()
                return {}
            raise AssertionError(operation)

        self.client.handler = handler
        self.server.receive({"jsonrpc": "2.0", "method": "notifications/initialized"})
        self.assertTrue(done.wait(2))
        finish = next(params for operation, params in self.client.calls if operation == "finish")
        self.assertEqual("uncertain", finish["outcome"])
        self.assertEqual(1, sum(op == "claim" for op, _ in self.client.calls))
        self.assertTrue(self.server.stop.is_set())

    def test_ping_and_initialize_cannot_reuse_an_in_flight_request_id(self):
        self.initialize()
        entered, release = threading.Event(), threading.Event()
        self.addCleanup(release.set)

        def handler(operation, params):
            entered.set()
            release.wait(5)
            return {"id": "accepted", "state": "queued"}

        self.client.handler = handler
        self.server.receive({"jsonrpc": "2.0", "id": "held", "method": "tools/call",
                             "params": {"name": "xordination_send", "arguments": {"to": "b", "body": "hello", "key": "k"}}})
        self.assertTrue(entered.wait(5))
        for method in ("ping", "initialize"):
            self.server.receive({"jsonrpc": "2.0", "id": "held", "method": method, "params": INITIALIZE})
        release.set()
        self.output.await_packet(lambda p: p.get("id") == "held" and "result" in p, timeout=5)
        replies = [packet for packet in self.output.packets() if packet.get("id") == "held"]
        self.assertEqual([-32600, -32600], [reply["error"]["code"] for reply in replies if "error" in reply])
        self.assertEqual(1, sum("result" in reply for reply in replies))

    def test_stdio_frame_limit_counts_encoded_bytes(self):
        # 300,000 four-byte characters stay under the character count but exceed the 1 MiB byte limit.
        frame = json.dumps({"jsonrpc": "2.0", "id": 1, "method": "ping", "pad": "\U0001F600" * 300000}, ensure_ascii=False)
        self.assertLess(len(frame), MAX_LINE)
        self.assertGreater(len(frame.encode()), MAX_LINE)
        data = (frame + "\n" + json.dumps({"jsonrpc": "2.0", "id": 2, "method": "initialize", "params": INITIALIZE}) + "\n").encode()
        output = io.StringIO()
        with patch("xordination.mcp.Client", return_value=self.client), \
             patch("sys.stdin", io.TextIOWrapper(io.BytesIO(data), encoding="utf-8")), patch("sys.stdout", output):
            self.assertEqual(0, run_stdio("http://localhost:8765", "test-token"))
        packets = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual(-32700, packets[0]["error"]["code"])
        self.assertEqual("2025-06-18", packets[1]["result"]["protocolVersion"])

    def test_stdio_parse_errors_recover_on_next_frame(self):
        input_lines = io.StringIO('{broken}\nNaN\n' + json.dumps({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": INITIALIZE}) + '\n')
        output = io.StringIO()
        with patch("xordination.mcp.Client", return_value=self.client), patch("sys.stdin", input_lines), patch("sys.stdout", output):
            self.assertEqual(0, run_stdio("http://localhost:8765", "test-token"))
        packets = [json.loads(line) for line in output.getvalue().splitlines()]
        self.assertEqual([-32700, -32700], [packet["error"]["code"] for packet in packets[:2]])
        self.assertEqual("2025-06-18", packets[2]["result"]["protocolVersion"])


if __name__ == "__main__":
    unittest.main()
