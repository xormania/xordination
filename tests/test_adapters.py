"""Native transport semantics, exercised with real owned mock subprocesses."""

import asyncio
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from xordination.adapters import (BrokerReconnect, CapabilityError, ClaudeChannelLog, CodexAdapter, GrokAdapter,
                                 claude_hook, doctor, run_adapter, run_claude)
from xordination.client import BrokerError, Client
from xordination.mcp import _Server
from xordination.rpc import MARKER, RPC, RPCError
from xordination.store import StoreError
from native_fixtures import FAMILY_NATIVE, MOCK_CODEX, MOCK_GROK, MOCK_CODEX_COMPLETED_BEFORE_RESPONSE


def crash_after_reading(script, methods, receipts):
    """Wrap a native fixture so it records, then dies on, the first frame naming one of methods.

    The frame has been written by the adapter and read by the native before the
    transport is lost, so the outcome is genuinely ambiguous.
    """
    return """
import json,os,sys
class _CrashAfterReading:
    def __init__(self, stream):
        self.stream = stream
    def __iter__(self):
        for line in self.stream:
            if json.loads(line).get("method") in %r:
                with open(%r, "a") as receipts:
                    receipts.write(line)
                os._exit(5)
            yield line
sys.stdin = _CrashAfterReading(sys.stdin)
""" % (tuple(methods), str(receipts)) + script


class ClientFixture:
    endpoint = "http://127.0.0.1:8765"
    token = "fixture-token"

    def __init__(self):
        self.calls = []
        self.tool_entered = threading.Event()
        self.tool_release = threading.Event()
        self.fail_finish = False
        self.statuses = {}

    def call(self, operation, **params):
        self.calls.append((operation, params))
        if operation == "bind":
            return {"native_id": params["native_id"], "capabilities": params.get("capabilities", {})}
        if operation == "finish":
            if self.fail_finish:
                raise ValueError("stale_attempt")
            return {"state": params["outcome"]}
        if operation == "wait":
            self.tool_entered.set()
            if not self.tool_release.wait(5):
                raise TimeoutError("Fixture wait not released")
            return {"messages": []}
        if operation == "claim":
            return {"message": None, "attempt": None, "attempt_id": None}
        if operation == "status":
            return self.statuses.get(params["message_id"], {})
        return {}




def message(identifier="message-one", attempt="attempt-one"):
    return {"id": identifier, "from": "sender", "from_generation": "s-one", "to": "recipient",
            "to_generation": "r-one", "delivery_generation": "r-one", "body": "Review this change", "reply_to": None,
            "metadata": {}, "attempt_id": attempt}


def fake_claude(folder, help_flags, accepts_channel,
                session="raise SystemExit('fixture received an unexpected session request')\n"):
    # Owned executable mirroring the observed parser: unknown options fail before input checks.
    path = Path(folder) / "claude"
    path.write_text("#!" + sys.executable + "\n" + "import sys\n"
        "args = sys.argv[1:]\n"
        "if args == ['--version']: print('2.1.302 (Claude Code)'); raise SystemExit(0)\n"
        "if args == ['--help']: print(" + repr(" ".join(help_flags)) + "); raise SystemExit(0)\n"
        "if '--dangerously-load-development-channels' in args and not " + repr(accepts_channel) + ":\n"
        "    print(\"error: unknown option '--dangerously-load-development-channels'\", file=sys.stderr)\n"
        "    raise SystemExit(1)\n"
        "if '--print' in args and not sys.stdin.read():\n"
        "    print('Error: Input must be provided either through stdin or as a prompt argument', file=sys.stderr)\n"
        "    raise SystemExit(1)\n"
        + session)
    path.chmod(0o700)
    return str(path)


class OrderedFinishClient(ClientFixture):
    """Hold each submission until both independent finishes are recorded."""

    def __init__(self, first_transport):
        super().__init__()
        self.first_transport = first_transport
        self.recorded = {transport: threading.Event() for transport in
                         ("grok/session/prompt", "grok/_x.ai/interject")}

    def call(self, operation, **params):
        transport = params.get("detail", {}).get("transport")
        if operation != "finish" or params.get("outcome") != "submitted":
            return super().call(operation, **params)
        if transport != self.first_transport and not self.recorded[self.first_transport].wait(5):
            raise TimeoutError("First finish did not reach its recording barrier")
        result = super().call(operation, **params)
        self.recorded[transport].set()
        if transport == self.first_transport:
            second = next(event for name, event in self.recorded.items() if name != transport)
            if not second.wait(5):
                raise TimeoutError("Second finish did not reach its recording barrier")
        return result

    def release(self):
        for event in self.recorded.values():
            event.set()


class NativeAdapterTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.client = ClientFixture()
        self.adapter = None
        self.store = None

    async def asyncTearDown(self):
        self.client.tool_release.set()
        if self.adapter:
            await self.adapter.close()
        if self.store:
            self.store.close()
        self.temporary.cleanup()

    def store_client(self):
        from xordination.store import Store
        self.store = Store(Path(self.temporary.name) / "broker.sqlite")
        self.store.create_space("project")
        sender = self.store.join("project", "sender", harness="codex", native_id="sender-thread")
        receiver = self.store.join("project", "recipient", harness="grok", native_id="grok-one")
        self.sender_context = self.store.authenticate(sender["token"])
        self.recipient_context = self.store.authenticate(receiver["token"])
        store, context = self.store, self.recipient_context
        class StoreClient(ClientFixture):
            def call(self, operation, **params):
                self.calls.append((operation, params))
                return getattr(store, operation)(context, **params)
        self.client = StoreClient()

    def stored_message(self, key):
        queued = self.store.send(self.sender_context, "recipient", "Review " + key, key)
        claimed = self.store.claim(self.recipient_context)
        self.assertEqual(claimed["message"]["id"], queued["id"])
        return {**claimed["message"], "attempt_id": claimed["attempt_id"]}

    async def process(self, script, adapter):
        process = await asyncio.create_subprocess_exec(sys.executable, "-u", "-c", script,
            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.DEVNULL, limit=4 * 1024 * 1024)
        adapter.rpc = RPC(process, adapter.request, adapter.notification, jsonrpc=isinstance(adapter, GrokAdapter))
        return process

    async def until(self, condition):
        async def poll():
            while not condition():
                await asyncio.sleep(.01)
        await asyncio.wait_for(poll(), 3)

    def submitted_finish(self, msg):
        matches = [params for operation, params in self.client.calls if operation == "finish"
                   and params["message_id"] == msg["id"] and params["attempt_id"] == msg["attempt_id"]
                   and params["outcome"] == "submitted"]
        self.assertEqual(len(matches), 1, "Expected one submission for this message and attempt")
        return matches[0]

    async def test_codex_busy_delivery_survives_held_tool_and_retains_thread(self):
        adapter = self.adapter = CodexAdapter(self.client, sys.executable, self.temporary.name)
        await self.process(MOCK_CODEX, adapter)
        adapter.session = "thread-one"
        await adapter.start_prompt("Continue existing project work")
        await self.until(self.client.tool_entered.is_set)
        msg = message()
        adapter.pending_attempts[msg["attempt_id"]] = msg
        await asyncio.wait_for(adapter.deliver(msg), 2)
        finished = next(params for op, params in self.client.calls if op == "finish")
        self.assertEqual(finished["outcome"], "submitted")
        self.assertEqual(finished["detail"]["native_id"], "thread-one")
        self.assertTrue(finished["detail"]["busy"])
        self.assertTrue(adapter.busy)
        self.assertEqual(adapter.active_turn, "turn-delivery")
        await adapter.rpc.request("fixture/late-completion", {})
        self.assertTrue(adapter.busy)
        self.assertEqual(adapter.active_turn, "turn-delivery")
        self.client.tool_release.set()

    async def test_codex_idle_uses_same_thread_and_shutdown_marks_unsettled_attempt(self):
        adapter = self.adapter = CodexAdapter(self.client, sys.executable, self.temporary.name)
        await self.process(MOCK_CODEX, adapter)
        adapter.session = "thread-one"
        msg = message()
        adapter.pending_attempts[msg["attempt_id"]] = msg
        await adapter.deliver(msg)
        self.assertFalse(next(params for op, params in self.client.calls if op == "finish")["detail"]["busy"])
        pending = message("not-settled", "pending-attempt")
        adapter.pending_attempts[pending["attempt_id"]] = pending
        process = adapter.rpc.process
        await adapter.close()
        self.assertIsNotNone(process.returncode)
        self.assertTrue(any(op == "finish" and params["message_id"] == "not-settled" and
                            params["outcome"] == "uncertain" for op, params in self.client.calls))

    async def test_codex_completed_event_before_start_response_never_reactivates_turn(self):
        # Both startup user turns and peer-delivery turns may finish before the
        # coroutine awaiting their native response gets its continuation.
        for operation in ("start_prompt", "deliver"):
            with self.subTest(operation=operation):
                adapter = self.adapter = CodexAdapter(self.client, sys.executable, self.temporary.name)
                await self.process(MOCK_CODEX_COMPLETED_BEFORE_RESPONSE, adapter)
                adapter.session = "thread-one"
                if operation == "start_prompt":
                    await adapter.start_prompt("An initial task")
                else:
                    msg = message()
                    adapter.pending_attempts[msg["attempt_id"]] = msg
                    await adapter.deliver(msg)
                self.assertFalse(adapter.busy)
                self.assertIsNone(adapter.active_turn)
                self.assertIn("finished-turn", adapter.completed_turns)
                await adapter.notification("turn/started", {"threadId": adapter.session, "turn": {"id": "new-turn"}})
                await adapter.notification("turn/started", {"threadId": adapter.session, "turn": {"id": "finished-turn"}})
                self.assertEqual(adapter.active_turn, "new-turn")
                self.assertTrue(adapter.busy)
                await adapter.close()

    async def test_codex_process_crash_is_uncertain_and_has_no_automatic_retry(self):
        adapter = self.adapter = CodexAdapter(self.client, sys.executable, self.temporary.name)
        await self.process(MOCK_CODEX, adapter)
        adapter.session = "thread-one"
        with self.assertRaises(ConnectionError):
            await adapter.rpc.request("fixture/crash", {})
        msg = message()
        adapter.pending_attempts[msg["attempt_id"]] = msg
        with self.assertRaises(ConnectionError):
            await adapter.deliver(msg)
        finished = [params for op, params in self.client.calls if op == "finish"]
        self.assertEqual(len(finished), 1)
        self.assertEqual(finished[0]["outcome"], "uncertain")
        self.assertFalse(any(op == "retry" for op, _ in self.client.calls))

    async def test_codex_start_preserves_native_configuration(self):
        adapter = self.adapter = CodexAdapter(self.client, sys.executable, self.temporary.name,
                                             model="operator-model", reasoning="high")
        captured = []
        parameters = {}
        async def spawn(argv, **kwargs):
            captured.append(argv)
            await self.process(MOCK_CODEX, adapter)
            original = adapter.rpc.request
            async def request(method, params, **options):
                parameters[method] = params
                return await original(method, params, **options)
            adapter.rpc.request = request
        adapter.spawn = spawn
        with patch("xordination.adapters.codex_capability", return_value={"status": "supported"}):
            await adapter.start()
        self.assertEqual(captured[0], [sys.executable, "app-server", "--listen", "stdio://"])
        self.assertNotIn("sandbox", parameters["thread/start"])
        self.assertNotIn("approvalPolicy", parameters["thread/start"])
        self.assertNotIn("developerInstructions", parameters["thread/start"])
        self.assertEqual(parameters["thread/start"]["config"], {"model_reasoning_effort": "high"})
        binding = next(params for op, params in self.client.calls if op == "bind")
        self.assertEqual(binding["capabilities"]["requested_model"], "operator-model")
        self.assertEqual(adapter.session, "thread-one")

    async def test_grok_busy_interject_does_not_create_new_session(self):
        adapter = self.adapter = GrokAdapter(self.client, sys.executable, self.temporary.name)
        await self.process(MOCK_GROK, adapter)
        adapter.session = "grok-one"
        first = message()
        adapter.pending_attempts[first["attempt_id"]] = first
        await adapter.deliver(first)
        self.assertTrue(adapter.busy)
        second = message("message-two", "attempt-two")
        adapter.pending_attempts[second["attempt_id"]] = second
        await adapter.deliver(second)
        finished = self.submitted_finish(second)
        self.assertEqual(finished["detail"]["transport"], "grok/_x.ai/interject")
        self.assertEqual(finished["detail"]["native_id"], "grok-one")
        await adapter.rpc.request("fixture/complete", {})
        await self.until(lambda: not adapter.tasks)
        self.assertTrue(adapter.busy)
        self.assertEqual(set(adapter.pending_interjections), {second["id"]})
        self.client.statuses[second["id"]] = {**second, "receipts": [{"message_id": second["id"],
            "generation": "replacement-generation", "observer": "recipient", "native_id": "grok-one"}], "replies": []}
        await adapter.refresh_delivery_state()
        self.assertTrue(adapter.busy)
        self.assertEqual(set(adapter.pending_interjections), {second["id"]})
        self.client.statuses[second["id"]]["receipts"][0]["generation"] = "r-one"
        await adapter.refresh_delivery_state()
        self.assertFalse(adapter.busy)
        self.assertEqual(len([op for op, _ in self.client.calls if op == "finish"]), 2)

    async def test_grok_finish_evidence_correlates_attempts_in_either_record_order(self):
        for first_transport in ("grok/session/prompt", "grok/_x.ai/interject"):
            with self.subTest(first_transport=first_transport):
                self.client = OrderedFinishClient(first_transport)
                adapter = self.adapter = GrokAdapter(self.client, sys.executable, self.temporary.name)
                try:
                    await self.process(MOCK_GROK, adapter)
                    adapter.session = "grok-one"
                    first, second = message(), message("message-two", "attempt-two")
                    for msg in (first, second):
                        adapter.pending_attempts[msg["attempt_id"]] = msg
                        await adapter.deliver(msg)
                    self.assertEqual(self.submitted_finish(first)["detail"]["transport"], "grok/session/prompt")
                    self.assertEqual(self.submitted_finish(second)["detail"]["transport"], "grok/_x.ai/interject")
                    recorded = [params["detail"]["transport"] for op, params in self.client.calls
                                if op == "finish" and params["outcome"] == "submitted"]
                    second_transport = next(name for name in self.client.recorded if name != first_transport)
                    self.assertEqual(recorded, [first_transport, second_transport])
                    await adapter.rpc.request("fixture/complete", {})
                    await self.until(lambda: adapter.prompts_in_flight == 0)  # noqa: B023
                finally:
                    self.client.release()
                    await adapter.close()

    async def test_managed_grok_loop_uses_receipt_to_release_interjection_barrier(self):
        self.store_client()
        adapter = self.adapter = GrokAdapter(self.client, sys.executable, self.temporary.name, poll_interval=.01)
        async def spawn(argv, **kwargs):
            await self.process(MOCK_GROK, adapter)
        adapter.spawn = spawn
        first = self.store.send(self.sender_context, "recipient", "Original task", "original")
        runner = asyncio.create_task(adapter.run())
        try:
            await self.until(lambda: self.store.status(self.recipient_context, first["id"])["state"] == "submitted")
            second = self.store.send(self.sender_context, "recipient", "Peer update", "interjected")
            await self.until(lambda: self.store.status(self.recipient_context, second["id"])["state"] == "submitted")
            self.assertEqual(self.store.status(self.recipient_context, second["id"])["attempts"][0]["detail"]["transport"],
                             "grok/_x.ai/interject")
            await adapter.rpc.request("fixture/complete", {})
            await self.until(lambda: adapter.prompts_in_flight == 0)
            self.assertTrue(adapter.busy)
            self.assertEqual(set(adapter.pending_interjections), {second["id"]})
            self.store.ack(self.recipient_context, second["id"])
            await self.until(lambda: not adapter.busy and not adapter.pending_interjections)
            third = self.store.send(self.sender_context, "recipient", "Next task", "after-receipt")
            await self.until(lambda: self.store.status(self.recipient_context, third["id"])["state"] == "submitted")
            self.assertEqual(self.store.status(self.recipient_context, third["id"])["attempts"][0]["detail"]["transport"],
                             "grok/session/prompt")
            await adapter.rpc.request("fixture/complete", {})
            await self.until(lambda: adapter.prompts_in_flight == 0)
        finally:
            runner.cancel()
            result = (await asyncio.gather(runner, return_exceptions=True))[0]
        self.assertIsInstance(result, asyncio.CancelledError)
        self.assertIsNotNone(adapter.rpc.process.returncode)
        recipient = next(peer for peer in self.store.peers(self.sender_context)["peers"]
                         if peer["participant"] == "recipient")
        self.assertFalse(recipient["active"])

    async def test_grok_unacknowledged_interjection_survives_original_prompt_completion(self):
        # Native prompt completion and authenticated receipt are independent.
        self.store_client()
        adapter = self.adapter = GrokAdapter(self.client, sys.executable, self.temporary.name)
        await self.process(MOCK_GROK, adapter)
        adapter.session = "grok-one"
        first = self.stored_message("original")
        adapter.pending_attempts[first["attempt_id"]] = first
        await adapter.deliver(first)
        interjection = self.stored_message("interjected")
        adapter.pending_attempts[interjection["attempt_id"]] = interjection
        await adapter.deliver(interjection)
        await adapter.rpc.request("fixture/complete", {})
        await self.until(lambda: not adapter.tasks)
        await adapter.refresh_delivery_state()
        self.assertEqual(adapter.prompts_in_flight, 0)
        self.assertFalse(adapter.native_busy)
        self.assertTrue(adapter.busy)
        self.assertEqual(set(adapter.pending_interjections), {interjection["id"]})
        third = self.stored_message("before-receipt")
        adapter.pending_attempts[third["attempt_id"]] = third
        await adapter.deliver(third)
        self.assertEqual(self.store.status(self.recipient_context, third["id"])["attempts"][0]["detail"]["transport"],
                         "grok/_x.ai/interject")
        self.store.ack(self.recipient_context, interjection["id"])
        await adapter.refresh_delivery_state()
        self.assertTrue(adapter.busy)  # The third interjection has its own barrier.
        self.store.send(self.recipient_context, "sender", "Third message received", "third-reply", reply_to=third["id"])
        await adapter.refresh_delivery_state()
        self.assertEqual(adapter.pending_interjections, {})
        self.assertFalse(adapter.busy)

    async def test_grok_old_prompt_response_cannot_idle_received_newer_interjected_turn(self):
        self.store_client()
        adapter = self.adapter = GrokAdapter(self.client, sys.executable, self.temporary.name)
        await self.process(MOCK_GROK, adapter)
        adapter.session = "grok-one"
        first = self.stored_message("original")
        adapter.pending_attempts[first["attempt_id"]] = first
        await adapter.deliver(first)
        old_id = adapter.prompt_id
        interjection = self.stored_message("interjected")
        adapter.pending_attempts[interjection["attempt_id"]] = interjection
        await adapter.deliver(interjection)
        self.store.ack(self.recipient_context, interjection["id"])
        await adapter.refresh_delivery_state()
        self.assertEqual(adapter.pending_interjections, {})
        await adapter.notification("session/update", {"sessionId": adapter.session,
            "update": {"sessionUpdate": "agent_thought_chunk", "prompt_id": "interjected-turn"}})
        await adapter.rpc.request("fixture/complete", {})
        await self.until(lambda: not adapter.tasks)
        await adapter.notification("_x.ai/session/prompt_complete", {"sessionId": adapter.session, "promptId": old_id})
        await adapter.notification("session/update", {"sessionId": adapter.session,
            "update": {"sessionUpdate": "tool_call_update", "prompt_id": old_id}})
        self.assertTrue(adapter.busy)
        self.assertTrue(adapter.native_busy)
        self.assertEqual(adapter.prompt_id, "interjected-turn")
        await adapter.notification("_x.ai/session/prompt_complete", {"sessionId": adapter.session, "promptId": "interjected-turn"})
        self.assertFalse(adapter.busy)

    async def test_grok_explicit_busy_rejection_is_failed_without_fallback_prompt(self):
        adapter = self.adapter = GrokAdapter(self.client, sys.executable, self.temporary.name)
        await self.process(MOCK_GROK, adapter)
        adapter.session = "grok-one"
        adapter.busy = True
        methods = []
        original = adapter.rpc.request
        async def rejected(method, params, **options):
            methods.append(method)
            if method == "_x.ai/interject":
                return {"accepted": False}
            return await original(method, params, **options)
        adapter.rpc.request = rejected
        msg = message()
        adapter.pending_attempts[msg["attempt_id"]] = msg
        with self.assertRaises(RPCError):
            await adapter.deliver(msg)
        finished = next(params for op, params in self.client.calls if op == "finish")
        self.assertEqual(finished["outcome"], "failed")
        self.assertEqual(methods, ["_x.ai/interject"])

    async def test_grok_late_completion_cannot_clear_newer_activity(self):
        adapter = GrokAdapter(self.client, sys.executable, self.temporary.name)
        old = adapter.started("old-turn")
        newer = adapter.started("new-turn")
        self.assertGreater(newer, old)
        adapter.completed("old-turn", epoch=old)
        self.assertTrue(adapter.busy)
        adapter.started("old-turn")
        self.assertEqual(adapter.prompt_id, "new-turn")
        adapter.completed("new-turn", epoch=newer)
        self.assertFalse(adapter.busy)
        # An anonymous newer epoch after native completion still fences the
        # captured epoch belonging to the original outstanding prompt response.
        original_epoch = adapter.started("original-anonymous-case")
        adapter.completed("original-anonymous-case")
        anonymous_epoch = adapter.started()
        self.assertGreater(anonymous_epoch, original_epoch)
        adapter.completed("original-anonymous-case", epoch=original_epoch)
        self.assertTrue(adapter.busy)
        self.assertIsNone(adapter.prompt_id)

    async def test_grok_prompt_crash_retains_uncertainty(self):
        adapter = self.adapter = GrokAdapter(self.client, sys.executable, self.temporary.name)
        await self.process(MOCK_GROK, adapter)
        adapter.session = "grok-one"
        msg = message()
        adapter.pending_attempts[msg["attempt_id"]] = msg
        await adapter.deliver(msg)
        with self.assertRaises(ConnectionError):
            await adapter.rpc.request("fixture/crash", {})
        await self.until(lambda: not adapter.tasks)
        finished = [params for op, params in self.client.calls if op == "finish"][-1]
        self.assertEqual(finished["outcome"], "uncertain")
        self.assertIsNotNone(adapter.delivery_failure)
        self.assertTrue(adapter.busy)

    async def test_unsettled_stale_finish_is_not_retried_natively(self):
        adapter = self.adapter = CodexAdapter(self.client, sys.executable, self.temporary.name)
        await self.process(MOCK_CODEX, adapter)
        adapter.session = "thread-one"
        self.client.fail_finish = True
        msg = message()
        adapter.pending_attempts[msg["attempt_id"]] = msg
        with self.assertRaisesRegex(ValueError, "stale_attempt"):
            await adapter.deliver(msg)
        self.assertIn(msg["attempt_id"], adapter.pending_attempts)
        self.assertFalse(any(op == "retry" for op, _ in self.client.calls))

    def broker_client(self, *, commit, before_retry=None):
        """The real store reached like the HTTP client: errors become BrokerError.

        The first finish loses its connection, after committing when ``commit``
        is set (only the response was lost). ``before_retry`` runs before the
        retried finish reaches the store.
        """
        from xordination.store import StoreError
        store, context = self.store, self.recipient_context
        class Lossy(ClientFixture):
            def call(self, operation, **params):
                self.calls.append((operation, params))
                finishes = sum(op == "finish" for op, _ in self.calls)
                if operation == "finish" and finishes == 1:
                    if commit:
                        store.finish(context, **params)
                    raise BrokerError("connection_error", "Broker connection lost: RemoteDisconnected")
                if operation == "finish" and finishes == 2 and before_retry:
                    before_retry()
                try:
                    return getattr(store, operation)(context, **params)
                except StoreError as error:
                    raise BrokerError(error.code, str(error)) from None
        return Lossy()

    async def lossy_delivery(self, key, **options):
        self.store_client()
        msg = self.stored_message(key)
        self.client = self.broker_client(**options)
        adapter = self.adapter = CodexAdapter(self.client, sys.executable, self.temporary.name)
        adapter.reconnect = BrokerReconnect(window=2, initial=.01)
        await self.process(MOCK_CODEX, adapter)
        adapter.session = "thread-one"
        adapter.pending_attempts[msg["attempt_id"]] = msg
        return adapter, msg

    async def test_lost_finish_response_settles_once_from_durable_attempt(self):
        adapter, msg = await self.lossy_delivery("lost-response", commit=True)
        await asyncio.wait_for(adapter.deliver(msg), 3)
        self.assertNotIn(msg["attempt_id"], adapter.pending_attempts)
        status = self.store.status(self.recipient_context, msg["id"])
        self.assertEqual(status["state"], "submitted")
        self.assertEqual([attempt["state"] for attempt in status["attempts"]], ["submitted"])
        # The retried evidence is retained as late, never as a second submission.
        self.assertEqual([event["late"] for event in status["attempts"][0]["evidence"]], [False, True])
        self.assertFalse(any(op in {"retry", "claim"} for op, _ in self.client.calls))

    async def test_reconnected_finish_of_replaced_generation_stays_fenced(self):
        replace = lambda: self.store.join("project", "recipient", harness="grok", replace=True)  # noqa: E731
        adapter, msg = await self.lossy_delivery("replaced-during-outage", commit=False, before_retry=replace)
        with self.assertRaises(BrokerError) as raised:
            await asyncio.wait_for(adapter.deliver(msg), 3)
        self.assertEqual(raised.exception.code, "stale_attempt")
        self.assertIn(msg["attempt_id"], adapter.pending_attempts)
        attempt = self.store.status(self.sender_context, msg["id"])["attempts"][0]
        self.assertEqual(attempt["state"], "uncertain")
        # Replacement settled the claim; the old generation's evidence is late.
        self.assertEqual([(event["outcome"], event["late"]) for event in attempt["evidence"]],
                         [("uncertain", False), ("submitted", True), ("uncertain", True)])

    async def test_broker_reconnect_is_bounded_and_never_retries_broker_answers(self):
        class Unreachable(ClientFixture):
            code = "connection_error"
            def call(self, operation, **params):
                self.calls.append((operation, params))
                raise BrokerError(self.code, "unavailable fixture")
        self.client = Unreachable()
        adapter = self.adapter = CodexAdapter(self.client, sys.executable, self.temporary.name)
        adapter.reconnect = BrokerReconnect(window=.3, initial=.02, maximum=.05)
        started = asyncio.get_running_loop().time()
        with self.assertRaises(BrokerError) as raised:
            await adapter.call("claim")
        self.assertEqual(raised.exception.code, "connection_error")
        self.assertGreaterEqual(asyncio.get_running_loop().time() - started, .25)
        self.assertGreater(len(self.client.calls), 2)
        for code in ("stale_generation", "unauthorized", "forbidden"):
            with self.subTest(code=code):
                self.client.calls.clear()
                self.client.code = code
                adapter.reconnect = BrokerReconnect(window=5, initial=.02)
                with self.assertRaises(BrokerError):
                    await adapter.call("claim")
                self.assertEqual(len(self.client.calls), 1)

    async def test_managed_codex_session_survives_broker_restart_and_keeps_fencing(self):
        from xordination.server import BrokerServer
        from xordination.store import Store
        path = Path(self.temporary.name) / "restart.sqlite"
        brokers = []
        def start(port=0):
            store = Store(str(path))
            server = BrokerServer(("127.0.0.1", port), store)
            thread = threading.Thread(target=server.serve_forever, kwargs={"poll_interval": .02}, daemon=True)
            thread.start()
            brokers.append((store, server, thread))
            return store, server.server_port
        def stop():
            store, server, thread = brokers.pop()
            server.stopping.set()
            server.shutdown()
            thread.join(timeout=2)
            server.server_close()
            store.close()
        store, port = start()
        try:
            store.create_space("project")
            sender = store.join("project", "sender")
            recipient = store.join("project", "recipient", harness="codex")
            sender_context = {"_token": sender["token"]}
            recipient_context = {"_token": recipient["token"]}
            adapter = self.adapter = CodexAdapter(Client("http://127.0.0.1:" + str(port), recipient["token"]),
                                                  sys.executable, self.temporary.name, poll_interval=.02)
            adapter.reconnect = BrokerReconnect(window=10, initial=.02, maximum=.1)
            async def spawn(argv, **kwargs):
                await self.process(MOCK_CODEX, adapter)
            adapter.spawn = spawn
            with patch("xordination.adapters.codex_capability", return_value={"status": "supported"}):
                runner = asyncio.create_task(adapter.run())
                first = store.send(sender_context, "recipient", "Before restart", "before")
                await self.until(lambda: store.status(recipient_context, first["id"])["state"] == "submitted")
            process = adapter.rpc.process
            stop()
            await asyncio.sleep(.3)  # Many claim polls fail while the broker is down.
            self.assertFalse(runner.done())
            self.assertIsNotNone(adapter.reconnect.since)
            self.assertIsNone(process.returncode)
            store, _ = start(port)
            second = store.send(sender_context, "recipient", "After restart", "after")
            await self.until(lambda: store.status(recipient_context, second["id"])["state"] == "submitted")
            self.assertIs(adapter.rpc.process, process)
            self.assertIsNone(adapter.reconnect.since)
            for queued in (first, second):
                attempts = store.status(recipient_context, queued["id"])["attempts"]
                self.assertEqual([(row["state"], row["native_id"]) for row in attempts],
                                 [("submitted", "thread-one")])  # Delivered once; no replay.
            # Restart preserved the binding generation; replacement still fences it.
            replacement = store.join("project", "recipient", harness="codex", replace=True)
            result = (await asyncio.wait_for(asyncio.gather(runner, return_exceptions=True), 3))[0]
            self.assertIsInstance(result, BrokerError)
            self.assertEqual(result.code, "stale_generation")
            self.assertIsNotNone(process.returncode)
            third = store.send(sender_context, "recipient", "For the replacement", "third")
            self.assertEqual(store.status({"_token": replacement["token"]}, third["id"])["attempts"], [])
        finally:
            while brokers:
                stop()

    async def test_nonowned_transport_cleanup_never_kills_foreign_process(self):
        async def reject(method, params):
            raise ValueError(method)
        async def ignore(method, params):
            return None
        process = await asyncio.create_subprocess_exec(sys.executable, "-u", "-c", "import time; time.sleep(30)",
                      stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
        rpc = RPC(process, reject, ignore, owned=False)
        try:
            await rpc.close()
            self.assertIsNone(process.returncode)
        finally:
            process.terminate()
            await process.wait()

    def lock_held(self, path):
        import fcntl
        with open(path, "a") as handle:
            try:
                fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                return True
            fcntl.flock(handle, fcntl.LOCK_UN)
            return False

    async def spawn_family(self, adapter, mode="polite"):
        # The lock is released only when the detached grandchild exits, which
        # avoids judging liveness by a PID that may be a zombie or reused.
        lock = str(Path(self.temporary.name) / ("family-" + mode + ".lock"))
        await adapter.spawn([sys.executable, "-u", "-c", FAMILY_NATIVE, lock, mode], jsonrpc=False)
        self.assertEqual((await adapter.rpc.request("fixture/held", {}, timeout=10))["held"], "held")
        self.assertTrue(self.lock_held(lock))
        return lock

    async def until_released(self, lock):
        async def poll():
            while self.lock_held(lock):
                await asyncio.sleep(.02)
        await asyncio.wait_for(poll(), 5)

    @unittest.skipUnless(os.name == "posix", "process groups are POSIX")
    async def test_adapter_stop_ends_detached_native_descendants(self):
        for mode in ("polite", "stubborn"):
            with self.subTest(mode=mode):
                adapter = self.adapter = CodexAdapter(self.client, sys.executable, self.temporary.name)
                lock = await self.spawn_family(adapter, mode)
                adapter.rpc.stop_grace = .5
                await adapter.close()
                await self.until_released(lock)
                self.assertIsNone(adapter.rpc.group)

    @unittest.skipUnless(sys.platform.startswith("linux"), "member scan reads /proc")
    async def test_reaped_leader_never_signals_a_reused_group_id(self):
        leader = await asyncio.create_subprocess_exec(sys.executable, "-c", "pass", start_new_session=True,
                                                      stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE)
        await leader.wait()
        # A foreign group now holds the ID the reaped leader had; stand in by pointing the RPC at it.
        foreign = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(30)"],
                                   start_new_session=True, env={**os.environ, MARKER: "foreign"})
        self.addCleanup(foreign.wait)
        self.addCleanup(foreign.kill)
        rpc = RPC(leader, lambda *args: None, lambda *args: None, jsonrpc=False, family="family-a")
        rpc.group = foreign.pid
        rpc.stop_grace = .2
        await rpc.close()
        self.assertIsNone(foreign.poll())
        self.assertIsNone(rpc.group)

    @unittest.skipUnless(os.name == "posix", "process groups are POSIX")
    async def test_native_crash_leaves_no_detached_descendants_after_adapter_cleanup(self):
        adapter = self.adapter = CodexAdapter(self.client, sys.executable, self.temporary.name)
        lock = await self.spawn_family(adapter)
        with self.assertRaises(ConnectionError):
            await adapter.rpc.request("fixture/crash", {})
        await adapter.rpc.process.wait()
        self.assertTrue(self.lock_held(lock), "The crashed native's grandchild outlives its parent")
        await adapter.close()
        await self.until_released(lock)

    @unittest.skipUnless(os.name == "posix", "process groups are POSIX")
    async def test_adapter_failure_after_spawn_stops_native_family(self):
        adapter = self.adapter = CodexAdapter(self.client, sys.executable, self.temporary.name)
        locks = []
        async def start():
            locks.append(await self.spawn_family(adapter))
            raise RuntimeError("adapter failed after native launch")
        adapter.start = start
        with self.assertRaisesRegex(RuntimeError, "after native launch"):
            await adapter.run()
        await self.until_released(locks[0])
        self.assertTrue(any(op == "stop" for op, _ in self.client.calls))

    @unittest.skipUnless(os.name == "posix", "process groups are POSIX")
    async def test_adapter_termination_signal_stops_native_family(self):
        # The native's own session no longer receives the terminal's signals;
        # SIGTERM to the adapter must run the same cleanup instead of orphaning it.
        lock = str(Path(self.temporary.name) / "signalled.lock")
        script = (
            "import asyncio,sys\n"
            "from xordination.adapters import CodexAdapter, supervise\n"
            "class Client:\n"
            "    def call(self, operation, **params):\n"
            "        return {}\n"
            "adapter = CodexAdapter(Client(), sys.executable, '.')\n"
            "async def start():\n"
            "    await adapter.spawn([sys.executable, '-u', '-c', sys.argv[1], sys.argv[2], 'polite'], jsonrpc=False)\n"
            "    await adapter.rpc.request('fixture/held', {}, timeout=10)\n"
            "    print('ready', flush=True)\n"
            "adapter.start = start\n"
            "sys.exit(asyncio.run(supervise(adapter)))\n")
        import xordination
        # Import the package under test from wherever this run imported it.
        env = {**os.environ, "PYTHONPATH": os.pathsep.join(
            [str(Path(xordination.__file__).resolve().parents[1]), os.environ.get("PYTHONPATH", "")])}
        child = subprocess.Popen([sys.executable, "-c", script, FAMILY_NATIVE, lock], cwd=self.temporary.name,
                                 env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        try:
            ready = await asyncio.wait_for(asyncio.to_thread(child.stdout.readline), 10)
            self.assertEqual(ready, b"ready\n")
            self.assertTrue(self.lock_held(lock))
            child.terminate()
            self.assertEqual(await asyncio.wait_for(asyncio.to_thread(child.wait), 10), 128 + 15)
            await self.until_released(lock)
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            child.stdout.close()

    @unittest.skipUnless(os.name == "posix", "process groups are POSIX")
    async def test_termination_stops_native_family_before_blocked_settlement(self):
        # A pending attempt's broker finish can block for the client timeout; the
        # family must not wait for it, or a SIGKILL escalation orphans it.
        lock = str(Path(self.temporary.name) / "settling.lock")
        record = Path(self.temporary.name) / "finish.json"
        script = (
            "import asyncio,json,sys,time\n"
            "from xordination.adapters import CodexAdapter, supervise\n"
            "class Client:\n"
            "    def call(self, operation, **params):\n"
            "        if operation == 'finish':\n"
            "            with open(sys.argv[3], 'w') as handle:\n"
            "                json.dump(params, handle)\n"
            "            time.sleep(30)\n"
            "        return {}\n"
            "adapter = CodexAdapter(Client(), sys.executable, '.')\n"
            "async def start():\n"
            "    await adapter.spawn([sys.executable, '-u', '-c', sys.argv[1], sys.argv[2], 'polite'], jsonrpc=False)\n"
            "    await adapter.rpc.request('fixture/held', {}, timeout=10)\n"
            "    adapter.pending_attempts['attempt-1'] = {'id': 'message-1', 'attempt_id': 'attempt-1'}\n"
            "    print('ready', flush=True)\n"
            "adapter.start = start\n"
            "sys.exit(asyncio.run(supervise(adapter)))\n")
        import xordination
        env = {**os.environ, "PYTHONPATH": os.pathsep.join(
            [str(Path(xordination.__file__).resolve().parents[1]), os.environ.get("PYTHONPATH", "")])}
        child = subprocess.Popen([sys.executable, "-c", script, FAMILY_NATIVE, lock, str(record)],
                                 cwd=self.temporary.name, env=env, stdout=subprocess.PIPE,
                                 stderr=subprocess.DEVNULL)
        try:
            ready = await asyncio.wait_for(asyncio.to_thread(child.stdout.readline), 10)
            self.assertEqual(ready, b"ready\n")
            self.assertTrue(self.lock_held(lock))
            child.terminate()
            await self.until_released(lock)
            async def settled():
                while not record.exists() or not record.read_text():
                    await asyncio.sleep(.02)
            await asyncio.wait_for(settled(), 5)
            # Settlement is still attempted after the stop, and only as uncertain.
            finish = json.loads(record.read_text())
            self.assertEqual((finish["attempt_id"], finish["outcome"]), ("attempt-1", "uncertain"))
            self.assertIsNone(child.poll(), "The adapter is still blocked in settlement")
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            child.stdout.close()


    @unittest.skipUnless(os.name == "posix", "process groups are POSIX")
    async def test_termination_during_cleanup_still_kills_stubborn_family(self):
        # The broker stopped the adapter, so cleanup is already in its grace
        # period when SIGTERM arrives; the signal must not cut it short.
        lock = str(Path(self.temporary.name) / "cleanup.lock")
        script = (
            "import asyncio,sys\n"
            "from xordination.adapters import CodexAdapter, supervise\n"
            "class Client:\n"
            "    def call(self, operation, **params):\n"
            "        return {'stopped': True} if operation == 'claim' else {}\n"
            "adapter = CodexAdapter(Client(), sys.executable, '.')\n"
            "async def start():\n"
            "    await adapter.spawn([sys.executable, '-u', '-c', sys.argv[1], sys.argv[2], 'stubborn'], jsonrpc=False)\n"
            "    await adapter.rpc.request('fixture/held', {}, timeout=10)\n"
            "    adapter.rpc.stop_grace = 1.5\n"
            "    send = adapter.rpc.send\n"
            "    def sent(group, kill):\n"
            "        send(group, kill)\n"
            "        print('killed' if kill else 'terminated', flush=True)\n"
            "    adapter.rpc.send = sent\n"
            "adapter.start = start\n"
            "sys.exit(asyncio.run(supervise(adapter)))\n")
        import xordination
        env = {**os.environ, "PYTHONPATH": os.pathsep.join(
            [str(Path(xordination.__file__).resolve().parents[1]), os.environ.get("PYTHONPATH", "")])}
        child = subprocess.Popen([sys.executable, "-c", script, FAMILY_NATIVE, lock], cwd=self.temporary.name,
                                 env=env, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        try:
            line = await asyncio.wait_for(asyncio.to_thread(child.stdout.readline), 10)
            self.assertEqual(line, b"terminated\n")
            self.assertTrue(self.lock_held(lock), "The grandchild ignores SIGTERM")
            child.terminate()
            self.assertEqual(await asyncio.wait_for(asyncio.to_thread(child.wait), 10), 128 + 15)
            # Checked at the supervisor's return, not after a later reprieve.
            self.assertFalse(self.lock_held(lock), "supervise returned while the family survived")
        finally:
            if child.poll() is None:
                child.kill()
                child.wait()
            child.stdout.close()


class ProbeAndChannelTests(unittest.TestCase):
    def test_mismatched_authenticated_harness_never_probes_launches_or_mutates_binding(self):
        from xordination.store import Store
        with tempfile.TemporaryDirectory() as folder:
            store = Store(Path(folder) / "broker.sqlite")
            try:
                store.create_space("project")
                for joined_harness, requested_harness in (("codex", "grok"), ("external", "codex")):
                    with self.subTest(joined=joined_harness, requested=requested_harness):
                        joined = store.join("project", "peer-" + joined_harness, harness=joined_harness,
                                            native_id="existing-" + joined_harness)
                        context = store.authenticate(joined["token"])
                        original = store.identity(context)
                        calls = []
                        class AuthenticatedClient:
                            def call(self, operation, **params):
                                calls.append(operation)  # noqa: B023
                                return getattr(store, operation)(context, **params)  # noqa: B023
                        with patch("xordination.client.Client", return_value=AuthenticatedClient()), \
                             patch("xordination.adapters._binary") as probe, \
                             patch("xordination.adapters.subprocess.Popen") as launch, \
                             patch("xordination.adapters.asyncio.create_subprocess_exec") as async_launch:
                            with self.assertRaisesRegex(CapabilityError, "authenticated participant harness"):
                                run_adapter(requested_harness, "http://127.0.0.1:1", joined["token"], folder)
                        probe.assert_not_called()
                        launch.assert_not_called()
                        async_launch.assert_not_called()
                        self.assertEqual(calls, ["identity"])
                        self.assertEqual(store.identity(context), original)
                        self.assertTrue(store.identity(context)["active"])
            finally:
                store.close()

    def test_unverified_attach_cannot_become_fresh_session(self):
        with self.assertRaisesRegex(CapabilityError, "attach/resume"):
            run_adapter("codex", "http://127.0.0.1:1", "token", ".", mode="attach", native_id="existing")

    def test_missing_binary_is_not_live_certification(self):
        observed = doctor("codex", binary="/missing/xordination-native-binary")
        self.assertEqual(observed["status"], "unavailable")
        self.assertEqual(observed["live_round_trip"], "unverified")

    def test_claude_doctor_accepts_listed_or_parser_accepted_channel_flag(self):
        listed = ("--session-id", "--mcp-config", "--debug-file", "--dangerously-load-development-channels")
        with tempfile.TemporaryDirectory() as folder:
            observed = doctor("claude", binary=fake_claude(folder, listed, accepts_channel=False))
            self.assertEqual(observed["status"], "supported")
            self.assertEqual(observed["launch"]["basis"], "installed CLI help; runtime handshake still required")
        with tempfile.TemporaryDirectory() as folder:
            observed = doctor("claude", binary=fake_claude(folder, listed[:3], accepts_channel=True))
            self.assertEqual(observed["status"], "supported")
            self.assertIn("unlisted channel flag accepted", observed["launch"]["basis"])
            self.assertEqual(observed["live_round_trip"], "unverified")

    def test_claude_doctor_rejected_channel_flag_remains_unverified(self):
        with tempfile.TemporaryDirectory() as folder:
            binary = fake_claude(folder, ("--session-id", "--mcp-config", "--debug-file"), accepts_channel=False)
            observed = doctor("claude", binary=binary)
            self.assertEqual(observed["status"], "unverified")
            self.assertEqual(observed["launch"]["basis"],
                             "installed CLI help; unlisted channel flag not confirmed by its option parser")
            client = ClientFixture()
            with self.assertRaisesRegex(CapabilityError, "unverified"):
                run_claude(client, binary, folder)
            self.assertEqual(client.calls, [])  # Refused before binding, so before any native launch.

    def test_claude_registration_waits_for_complete_fresh_line(self):
        with tempfile.TemporaryDirectory() as folder:
            path = Path(folder) / "native-debug.log"
            observer = ClaudeChannelLog(path)
            self.assertFalse(observer.poll())
            marker = '2026-10-10T20:00:00.000Z [DEBUG] MCP server "xordination": Channel notifications registered'
            path.write_text(marker)
            self.assertFalse(observer.poll())
            with path.open("a") as stream:
                stream.write("\n")
            self.assertTrue(observer.poll())

    def test_claude_supervisor_survives_broker_outage(self):
        class Outage(ClientFixture):
            """Two lost identity polls and a lost registration bind, then an explicit stop."""
            def call(self, operation, **params):
                self.calls.append((operation, params))
                registrations = sum(op == "bind" and row["capabilities"].get("channel_registered") is True
                                    for op, row in self.calls)
                if operation == "identity":
                    if sum(op == "identity" for op, _ in self.calls) <= 2:
                        raise BrokerError("connection_error", "Broker connection lost: ConnectionRefusedError")
                    return {"stopped": True} if registrations >= 2 or len(self.calls) > 200 else {"active": True}
                if operation == "bind" and registrations == 1:
                    raise BrokerError("unavailable", "Broker operation unavailable")
                return {}
        with tempfile.TemporaryDirectory() as folder:
            binary = Path(folder) / "claude"
            binary.write_text("#!" + sys.executable + "\nimport sys, time\n"
                "log = sys.argv[sys.argv.index('--debug-file') + 1]\n"
                "open(log, 'w').write('x [DEBUG] MCP server \"xordination\": Channel notifications registered\\n')\n"
                "time.sleep(30)\n")
            binary.chmod(0o700)
            client = Outage()
            with patch("xordination.adapters.doctor", return_value={"status": "supported"}):
                self.assertEqual(run_claude(client, str(binary), folder), 0)
        registrations = [params for op, params in client.calls
                         if op == "bind" and params["capabilities"].get("channel_registered")]
        self.assertEqual(len(registrations), 2)  # Lost once, then recorded after reconnect.
        self.assertEqual(client.calls[-1][0], "stop")

    def test_client_reports_dropped_broker_connection_as_connection_error(self):
        import socket
        listener = socket.socket()
        listener.bind(("127.0.0.1", 0))
        listener.listen(1)
        def drop():
            connection, _ = listener.accept()
            connection.recv(65536)
            connection.close()  # Request accepted, response never sent.
        thread = threading.Thread(target=drop, daemon=True)
        thread.start()
        try:
            with self.assertRaises(BrokerError) as raised:
                Client("http://127.0.0.1:" + str(listener.getsockname()[1]), "token", timeout=5).call("claim")
            self.assertEqual(raised.exception.code, "connection_error")
        finally:
            thread.join(timeout=5)
            listener.close()

    def test_claude_hook_reports_observed_identity_without_tool_decision(self):
        client = ClientFixture()
        with patch("xordination.client.Client", return_value=client):
            self.assertEqual(claude_hook("http://127.0.0.1:1", "token",
                packet={"session_id": "actual-native", "hook_event_name": "SessionStart"}), 0)
        self.assertEqual(client.calls[0][0], "bind")
        self.assertEqual(client.calls[0][1]["native_id"], "actual-native")
        self.assertTrue(client.calls[0][1]["capabilities"]["identity_observed"])


CLAUDE_FLAGS = ("--session-id", "--mcp-config", "--debug-file", "--dangerously-load-development-channels")


def recording_native(folder, name, script):
    """An owned executable recording its launch and every received frame, then running a scripted native."""
    path = Path(folder) / name
    path.write_text("#!" + sys.executable + "\n" + r'''
import json, os, sys
_record = open(os.environ["XORDINATION_FIXTURE_RECORD"], "a")
_record.write(json.dumps({"argv": sys.argv, "cwd": os.getcwd(), "env": {key: os.environ.get(key) for key in
    ("HOME", "GROK_HOME", "CLAUDE_CONFIG_DIR", "OPERATOR_SETTING", "XORDINATION_TOKEN")}}) + "\n")
_record.flush()
class _Received:
    def __init__(self, stream):
        self.stream = stream
    def __iter__(self):
        for line in self.stream:
            _record.write(line)
            _record.flush()
            yield line
sys.stdin = _Received(sys.stdin)
''' + script)
    path.chmod(0o700)
    return str(path)


def tree_snapshot(root):
    """Content and mode of every operator-owned file, to detect any setup mutation."""
    root = Path(root)
    return {str(path.relative_to(root)): (path.read_bytes(), path.stat().st_mode)
            for path in sorted(root.rglob("*")) if path.is_file()}


def operator_project(root, harness):
    """A project whose operator already has partial native configuration and a usable tool."""
    project, home = Path(root) / "project", Path(root) / "home"
    files = {project / "tools" / "check.sh": "#!/bin/sh\nexit 0\n",
             project / "AGENTS.md": "Operator conventions stay authoritative.\n"}
    if harness == "claude":
        files.update({
            project / ".claude" / "settings.json": json.dumps({
                "model": "operator-model", "permissions": {"allow": ["Bash(tools/check.sh)"], "defaultMode": "plan"},
                "hooks": {"PreToolUse": [{"matcher": "Bash", "hooks": [{"type": "command", "command": "tools/check.sh"}]}]}}),
            project / ".claude" / "settings.local.json": json.dumps({"env": {"OPERATOR_FLAG": "1"}}),
            project / ".mcp.json": json.dumps({"mcpServers": {"project-tools": {"command": "tools/check.sh"}}}),
            home / "settings.json": json.dumps({"hooks": {"Stop": [{"hooks": [{"type": "command", "command": "true"}]}]}}),
        })
    else:
        files.update({
            project / ".grok" / "settings.json": json.dumps({"mcpServers": {"project-tools": {"command": "tools/check.sh"}}}),
            home / "config.toml": 'model = "operator-model"\n[permissions]\ndefault = "ask"\n',
        })
    for path, text in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    (project / "tools" / "check.sh").chmod(0o755)
    return project, home


class ConfigurationPreservationTests(unittest.IsolatedAsyncioTestCase):
    """Launch adds communication only; partial operator settings, MCP entries, hooks and tools stay native (AC-13)."""

    # Launch options that would replace or narrow operator-owned native configuration.
    OVERRIDES = ("--strict-mcp-config", "--setting-sources", "--permission-mode", "--dangerously-skip-permissions",
                 "--allowedTools", "--allowed-tools", "--disallowedTools", "--disallowed-tools", "--tools",
                 "--model", "--effort", "--bare", "--sandbox", "--yolo")

    async def asyncSetUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.record = self.root / "native-record.jsonl"
        (self.root / "bin").mkdir()

    async def asyncTearDown(self):
        self.temporary.cleanup()

    def recorded(self):
        rows = [json.loads(line) for line in self.record.read_text().splitlines()]
        return [row for row in rows if "argv" in row], [row for row in rows if "argv" not in row]

    def assert_preserved(self, snapshot, project, home, launch):
        self.assertEqual(tree_snapshot(project), snapshot["project"])
        self.assertEqual(tree_snapshot(home), snapshot["home"])
        self.assertEqual(launch["cwd"], str(project.resolve()))
        self.assertEqual((launch["env"].get("HOME"), launch["env"].get("OPERATOR_SETTING")), (str(home), "kept"))
        self.assertFalse(set(launch["argv"]) & set(self.OVERRIDES))

    async def test_claude_launch_preserves_partial_operator_configuration_and_project_tools(self):
        project, home = operator_project(self.root, "claude")
        snapshot = {"project": tree_snapshot(project), "home": tree_snapshot(home)}
        session = (
            "import json, os\n"
            "flag = lambda name: args[args.index(name) + 1]\n"
            "keys = ('HOME', 'CLAUDE_CONFIG_DIR', 'OPERATOR_SETTING', 'XORDINATION_TOKEN')\n"
            "launch = {'argv': args, 'cwd': os.getcwd(), 'env': {key: os.environ.get(key) for key in keys},\n"
            "          'mcp': json.load(open(flag('--mcp-config'))), 'settings': json.load(open(flag('--settings'))),\n"
            "          'injected': [flag('--mcp-config'), flag('--settings')],\n"
            "          'tool_usable': os.access('tools/check.sh', os.X_OK)}\n"
            "open(os.environ['XORDINATION_FIXTURE_RECORD'], 'a').write(json.dumps(launch) + '\\n')\n")
        binary = fake_claude(self.root / "bin", CLAUDE_FLAGS, accepts_channel=True, session=session)
        client = ClientFixture()
        environment = {"HOME": str(home), "CLAUDE_CONFIG_DIR": str(home), "OPERATOR_SETTING": "kept",
                       "XORDINATION_FIXTURE_RECORD": str(self.record)}
        with patch.dict("os.environ", environment):
            self.assertEqual(await asyncio.to_thread(run_claude, client, binary, project), 0)
        [launch], _ = self.recorded()
        self.assert_preserved(snapshot, project, home, launch)
        self.assertEqual(launch["env"]["CLAUDE_CONFIG_DIR"], str(home))
        self.assertEqual(launch["env"]["XORDINATION_TOKEN"], client.token)
        self.assertTrue(launch["tool_usable"])
        # Communication is added beside, not instead of, the operator's MCP servers and hooks.
        self.assertEqual(set(launch["mcp"]["mcpServers"]), {"xordination"})
        self.assertEqual(set(launch["settings"]), {"hooks"})
        self.assertEqual(set(launch["settings"]["hooks"]), {"SessionStart"})
        [entry] = launch["settings"]["hooks"]["SessionStart"]
        self.assertEqual(set(entry), {"hooks"})
        self.assertIn(" claude-hook ", entry["hooks"][0]["command"])
        # The injected files were temporary and outside the project.
        for path in launch["injected"]:
            self.assertNotIn(project.resolve(), Path(path).resolve().parents)
            self.assertFalse(Path(path).exists())

    async def test_grok_start_preserves_partial_operator_configuration_and_project_tools(self):
        project, home = operator_project(self.root, "grok")
        snapshot = {"project": tree_snapshot(project), "home": tree_snapshot(home)}
        binary = recording_native(self.root / "bin", "grok", MOCK_GROK)
        client = ClientFixture()
        environment = {"HOME": str(home), "GROK_HOME": str(home), "OPERATOR_SETTING": "kept",
                       "XORDINATION_FIXTURE_RECORD": str(self.record)}
        adapter = GrokAdapter(client, binary, project)
        try:
            with patch.dict("os.environ", environment):
                await adapter.start()
        finally:
            await adapter.close()
        [launch], received = self.recorded()
        self.assert_preserved(snapshot, project, home, launch)
        self.assertEqual(launch["argv"][1:], ["--no-auto-update", "agent", "stdio"])
        self.assertEqual(launch["env"]["GROK_HOME"], str(home))
        self.assertEqual(launch["env"]["XORDINATION_TOKEN"], client.token)
        # No authentication change, mode/model switch or tool policy accompanies session creation.
        self.assertEqual([frame["method"] for frame in received], ["initialize", "session/new"])
        created = received[1]["params"]
        self.assertEqual(set(created), {"cwd", "mcpServers"})
        self.assertEqual(created["cwd"], str(project.resolve()))
        self.assertEqual([server["name"] for server in created["mcpServers"]], ["xordination"])
        self.assertEqual(set(created["mcpServers"][0]), {"name", "command", "args", "env"})


class BrokerClient(ClientFixture):
    """Real broker authority for one captured credential, with injectable broker faults."""

    def __init__(self, store, token):
        super().__init__()
        self.store, self.token = store, token
        self.context = store.authenticate(token)
        self.after_claim = None

    def call(self, operation, **params):
        self.calls.append((operation, params))
        if operation == "finish" and self.fail_finish:
            raise StoreError("stale_attempt", "Injected stale durable finish")
        result = getattr(self.store, operation)(self.context, **params)
        if operation == "claim" and result.get("message") and self.after_claim:
            self.after_claim()
        return result


class AdapterConformance:
    """Shared AC-12 failure contracts; each harness supplies only its native fixture.

    Subclasses provide launch(), launch_failing_native(), refuse_capability(),
    quiesced() and shutdown(), and record native submissions and session
    creations. Every scenario uses a real SQLite broker and recipient credential.
    """

    harness = None
    launch_sessions = 0
    refused_state = "failed"

    async def asyncSetUp(self):
        from xordination.store import Store
        self.temporary = tempfile.TemporaryDirectory()
        self.store = Store(Path(self.temporary.name) / "broker.sqlite")
        self.store.create_space("project")
        self.sender = self.store.authenticate(self.store.join("project", "sender", harness="codex")["token"])
        self.recipient = self.store.join("project", "recipient", harness=self.harness)
        self.client = BrokerClient(self.store, self.recipient["token"])
        self.submissions, self.sessions = [], []
        self.break_on_submit = self.refuse_on_submit = False

    async def asyncTearDown(self):
        await self.shutdown()
        self.store.close()
        self.temporary.cleanup()

    def send(self, key):
        return self.store.send(self.sender, "recipient", "Review " + key, key)

    def status(self, message):
        return self.store.status(self.sender, message["id"])

    async def until(self, condition, timeout=5):
        async def poll():
            while not condition():
                await asyncio.sleep(.01)
        try:
            await asyncio.wait_for(poll(), timeout)
        except asyncio.TimeoutError:
            self.fail("Harness did not reach the expected broker/native state")

    def assert_not_replayed(self, message, states):
        status = self.status(message)
        self.assertIn(status["state"], states)
        self.assertEqual(len(status["attempts"]), 1)
        self.assertEqual(len(self.submissions), 1, "The native received exactly one submission")
        self.assertEqual(len(self.sessions), self.launch_sessions, "No fallback native session was created")
        self.assertFalse(any(operation == "retry" for operation, _ in self.client.calls))
        return status

    def assert_native_read_delivery(self):
        """Harnesses with a native process prove the lost delivery frame was read before the loss."""

    async def test_startup_failure_never_reports_readiness(self):
        queued = self.send("before-startup")
        outcome = await self.launch_failing_native()
        self.assertTrue(isinstance(outcome, Exception) or outcome != 0, outcome)
        binding = self.store.authenticate(self.recipient["token"], allow_retired=True)
        self.assertFalse(binding["active"])
        self.assertIsNot(binding["capabilities"].get("channel_registered"), True)
        self.assertEqual(binding["capabilities"].get("live_round_trip", "unverified"), "unverified")
        status = self.status(queued)
        self.assertEqual((status["state"], status["attempts"]), ("queued", []))
        self.assertEqual(self.submissions, [])

    async def test_ambiguous_submission_is_uncertain_without_retry(self):
        queued = self.send("ambiguous")
        self.break_on_submit = True
        await self.launch()
        await self.until(lambda: self.status(queued)["state"] == "uncertain" and self.quiesced())
        status = self.assert_not_replayed(queued, {"uncertain"})
        self.assertTrue(status["unresolved_uncertainty"])
        self.assertEqual(status["attempts"][0]["state"], "uncertain")
        self.assert_native_read_delivery()

    async def test_stale_finish_is_not_replayed(self):
        queued = self.send("stale")
        self.client.fail_finish = True
        await self.launch()
        await self.until(lambda: self.submissions and self.quiesced())
        # The durable claim stays with broker recovery; it is never settled or resubmitted.
        status = self.assert_not_replayed(queued, {"claimed", "uncertain"})
        self.assertFalse(any(event["outcome"] == "submitted" for event in status["attempts"][0]["evidence"]))

    async def test_replacement_fences_late_native_evidence(self):
        queued = self.send("replaced")
        replacement = {}
        def replace():
            self.client.after_claim = None
            replacement.update(self.store.join("project", "recipient", harness=self.harness, replace=True))
        self.client.after_claim = replace
        await self.launch()
        def late():
            evidence = self.status(queued)["attempts"][0]["evidence"] if replacement else []
            return any(event["late"] and event["outcome"] == "submitted" for event in evidence)
        await self.until(lambda: late() and self.quiesced())
        status = self.assert_not_replayed(queued, {"uncertain"})
        self.assertTrue(all(event["late"] for event in status["attempts"][0]["evidence"]
                            if event["outcome"] == "submitted"))
        with self.assertRaises(StoreError) as raised:
            self.store.claim(self.client.context)
        self.assertEqual(raised.exception.code, "stale_generation")
        current = self.store.authenticate(replacement["token"])
        self.assertIsNone(self.store.claim(current)["message"], "Handover requires explicit recovery")
        self.assertEqual((current["native_id"], current["capabilities"]), (None, {}))

    async def test_capability_refusal_has_no_fallback_submission(self):
        with self.assertRaisesRegex(CapabilityError, "attach/resume"):
            run_adapter(self.harness, "http://127.0.0.1:1", self.recipient["token"], self.temporary.name,
                        mode="attach", native_id="existing-native")
        queued = self.send("refused")
        await self.refuse_capability()
        await self.until(self.quiesced)
        status = self.status(queued)
        self.assertEqual(status["state"], self.refused_state)
        self.assertEqual([attempt["state"] for attempt in status["attempts"]],
                         [] if self.refused_state == "queued" else [self.refused_state])
        self.assertEqual(len(self.submissions), len(status["attempts"]), "Only the refused submission was made")
        self.assertEqual(len(self.sessions), self.launch_sessions, "No fallback native session was created")
        self.assertFalse(any(operation == "retry" for operation, _ in self.client.calls))


class NativeRPCConformance(AdapterConformance):
    """Managed Codex/Grok loops against their scripted native subprocesses."""

    launch_sessions = 1
    native_script = None

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.runner = None
        self.native_receipts = Path(self.temporary.name) / "native-receipts.jsonl"

    def assert_native_read_delivery(self):
        # The native itself read exactly the one delivery frame before its transport was lost.
        frames = self.native_receipts.read_text().splitlines() if self.native_receipts.exists() else []
        self.assertEqual([json.loads(frame)["method"] for frame in frames], self.submissions)

    async def launch(self):
        adapter = self.adapter = self.adapter_class(self.client, sys.executable, self.temporary.name,
                                                    poll_interval=.01)
        script = self.native_script or self.script
        if self.break_on_submit:
            script = crash_after_reading(script, self.submission_methods, self.native_receipts)
        async def spawn(argv, **kwargs):
            process = await asyncio.create_subprocess_exec(sys.executable, "-u", "-c", script,
                stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL, limit=4 * 1024 * 1024)
            adapter.rpc = RPC(process, adapter.request, adapter.notification, jsonrpc=self.jsonrpc)
            native = adapter.rpc.request
            async def request(method, params, **options):
                if method in self.session_methods:
                    self.sessions.append(method)
                if method in self.submission_methods:
                    self.submissions.append(method)
                    if self.refuse_on_submit:
                        raise RPCError({"code": -32601, "message": "Native delivery method is not supported"})
                return await native(method, params, **options)
            adapter.rpc.request = request
        adapter.spawn = spawn
        self.runner = asyncio.create_task(adapter.run())

    async def launch_failing_native(self):
        self.native_script = "import sys; sys.exit(3)"
        await self.launch()
        return (await asyncio.gather(self.runner, return_exceptions=True))[0]

    async def refuse_capability(self):
        self.refuse_on_submit = True
        await self.launch()

    def quiesced(self):
        return self.runner is not None and self.runner.done()

    async def shutdown(self):
        if self.runner:
            self.runner.cancel()
            await asyncio.gather(self.runner, return_exceptions=True)


class CodexConformanceTests(NativeRPCConformance, unittest.IsolatedAsyncioTestCase):
    harness, adapter_class, script, jsonrpc = "codex", CodexAdapter, MOCK_CODEX, False
    submission_methods, session_methods = ("turn/start",), ("thread/start",)

    async def asyncSetUp(self):
        await super().asyncSetUp()
        probe = patch("xordination.adapters.codex_capability", return_value={"status": "supported"})
        probe.start()
        self.addCleanup(probe.stop)


class GrokConformanceTests(NativeRPCConformance, unittest.IsolatedAsyncioTestCase):
    harness, adapter_class, script, jsonrpc = "grok", GrokAdapter, MOCK_GROK, True
    submission_methods, session_methods = ("session/prompt", "_x.ai/interject"), ("session/new",)


class ChannelOutput:
    """Claude channel stdout: records each attempted frame and may close as one is written."""

    def __init__(self, test):
        self.test = test
        self.after_frame = None

    def write(self, text):
        method = json.loads(text).get("method")
        if method != "notifications/claude/channel":
            return len(text)
        self.test.submissions.append(method)
        self.after_frame = len(self.test.client.calls)
        if self.test.break_on_submit:
            raise BrokenPipeError("Claude channel closed while the frame was written")
        return len(text)

    def flush(self):
        pass


class ClaudeChannelConformanceTests(AdapterConformance, unittest.IsolatedAsyncioTestCase):
    """The Claude delivery path: the MCP channel frontend after fresh registration."""

    harness, refused_state = "claude", "queued"

    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.server = None

    async def launch(self):
        self.output = ChannelOutput(self)
        self.server = _Server(self.client, self.output, channel=True, error_output=io.StringIO())
        self.server.dispatch("initialize", {"protocolVersion": "2025-06-18", "capabilities": {},
                                            "clientInfo": {"name": "claude-fixture", "version": "1"}}, threading.Event())
        # run_claude publishes this only after observing fresh native handler registration.
        await asyncio.to_thread(self.client.call, "bind", native_id="claude-session",
                                capabilities={"channel_registered": True})
        self.server.receive({"jsonrpc": "2.0", "method": "notifications/initialized"})

    async def launch_failing_native(self):
        binary = fake_claude(self.temporary.name, CLAUDE_FLAGS, accepts_channel=True, session="raise SystemExit(3)\n")
        return await asyncio.to_thread(run_claude, self.client, binary, self.temporary.name)

    async def refuse_capability(self):
        binary = fake_claude(self.temporary.name, CLAUDE_FLAGS[:3], accepts_channel=False)
        with self.assertRaisesRegex(CapabilityError, "unverified"):
            await asyncio.to_thread(run_claude, self.client, binary, self.temporary.name)
        self.assertEqual(self.client.calls, [])  # Refused before binding or launching.

    def quiesced(self):
        if self.server is None or self.server.stop.is_set():
            return True
        # The channel loop has polled the broker again after its single frame.
        after = self.output.after_frame
        return after is not None and any(operation == "identity" for operation, _ in self.client.calls[after:])

    async def shutdown(self):
        if self.server:
            await asyncio.to_thread(self.server.close)


if __name__ == "__main__":
    unittest.main()
