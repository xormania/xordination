"""Native transport semantics, exercised with real owned mock subprocesses."""

import asyncio
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import threading
import unittest
from unittest.mock import patch

from xordination.adapters import (CapabilityError, ClaudeChannelLog, CodexAdapter, GrokAdapter,
                                 claude_hook, doctor, run_adapter, run_claude)
from xordination.rpc import RPC, RPCError
from native_fixtures import MOCK_CODEX, MOCK_GROK, MOCK_CODEX_COMPLETED_BEFORE_RESPONSE


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
                    await self.until(lambda: adapter.prompts_in_flight == 0)
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
                                calls.append(operation)
                                return getattr(store, operation)(context, **params)
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

    def fake_claude(self, folder, help_flags, accepts_channel):
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
            "raise SystemExit('fixture received an unexpected session request')\n")
        path.chmod(0o700)
        return str(path)

    def test_claude_doctor_accepts_listed_or_parser_accepted_channel_flag(self):
        listed = ("--session-id", "--mcp-config", "--debug-file", "--dangerously-load-development-channels")
        with tempfile.TemporaryDirectory() as folder:
            observed = doctor("claude", binary=self.fake_claude(folder, listed, accepts_channel=False))
            self.assertEqual(observed["status"], "supported")
            self.assertEqual(observed["launch"]["basis"], "installed CLI help; runtime handshake still required")
        with tempfile.TemporaryDirectory() as folder:
            observed = doctor("claude", binary=self.fake_claude(folder, listed[:3], accepts_channel=True))
            self.assertEqual(observed["status"], "supported")
            self.assertIn("unlisted channel flag accepted", observed["launch"]["basis"])
            self.assertEqual(observed["live_round_trip"], "unverified")

    def test_claude_doctor_rejected_channel_flag_remains_unverified(self):
        with tempfile.TemporaryDirectory() as folder:
            binary = self.fake_claude(folder, ("--session-id", "--mcp-config", "--debug-file"), accepts_channel=False)
            observed = doctor("claude", binary=binary)
            self.assertEqual(observed["status"], "unverified")
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

    def test_claude_hook_reports_observed_identity_without_tool_decision(self):
        client = ClientFixture()
        with patch("xordination.client.Client", return_value=client):
            self.assertEqual(claude_hook("http://127.0.0.1:1", "token",
                packet={"session_id": "actual-native", "hook_event_name": "SessionStart"}), 0)
        self.assertEqual(client.calls[0][0], "bind")
        self.assertEqual(client.calls[0][1]["native_id"], "actual-native")
        self.assertTrue(client.calls[0][1]["capabilities"]["identity_observed"])


if __name__ == "__main__":
    unittest.main()
