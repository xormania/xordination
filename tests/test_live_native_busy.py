"""Real observed adapter/RPC/broker busy orchestration with finite native models.

This exercises the live runner's machinery. The executables remain scripted
protocol fixtures and do not establish vendor or model compatibility.
"""

import asyncio
import importlib.util
import json
import os
from pathlib import Path
import sys
import tempfile
import unittest
from unittest.mock import patch

from helpers import isolated_process_env, owned_broker
from live_native_fixture import CODEX_ACCEPTANCE
from xordination.client import Client
from xordination.rpc import RPC


SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "testing" / "live.py"
spec = importlib.util.spec_from_file_location("live_native_busy_runner", SCRIPT)
live = importlib.util.module_from_spec(spec)
spec.loader.exec_module(live)


class LiveNativeBusyTests(unittest.IsolatedAsyncioTestCase):
    async def replies_submitted(self, run, replies):
        while True:
            with live.readonly(run / "broker.sqlite3") as connection:
                submitted = {row["body"] for row in connection.execute(
                    "SELECT DISTINCT messages.body FROM messages JOIN attempts ON attempts.message_id=messages.id "
                    "WHERE attempts.state='submitted'")}
            if set(replies) <= submitted:
                return
            await asyncio.sleep(.02)

    async def exercise_busy(self, *, recipient_mode="gate", phase_timeout=10, scenarios=("busy",), mutate_wait_boundary=False):
        with tempfile.TemporaryDirectory() as directory:
            directory = Path(directory)
            run = directory / "run"
            live.prepare({"participants": [{"name": "alice", "harness": "codex", "version": "protocol-fixture"},
                                           {"name": "bob", "harness": "codex", "version": "protocol-fixture"}],
                          "pairs": [["alice", "bob"]], "scenarios": list(scenarios)}, run)
            adapters, tasks = [], []
            prepared = live.load(run / "prepared.json")
            completed = {case["reply"]: asyncio.Event() for case in prepared["cases"]}
            with owned_broker(run / "broker.sqlite3") as (_, _, endpoint):
                try:
                    for name in ("alice", "bob"):
                        connection = live.load(run / (name + ".connection.json"))
                        adapter = live.observed_adapter(run, name, Client(endpoint, connection["token"]),
                                                        sys.executable, directory)
                        adapters.append(adapter)
                        adapter.session = "native-" + name
                        adapter.prompt = None
                        adapter.poll_interval = .01
                        await adapter.bind(activity="idle", transport="owned-protocol-fixture")
                        native_home = directory / (name + "-home")
                        native_home.mkdir()
                        process = await asyncio.create_subprocess_exec(
                            sys.executable, "-u", "-c", CODEX_ACCEPTANCE, adapter.session,
                            recipient_mode if name == "bob" else "gate",
                            stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE,
                            stderr=asyncio.subprocess.PIPE, env=isolated_process_env(native_home))
                        original = adapter.notification
                        idle = asyncio.Event()
                        async def observed(method, params, *, original=original, idle=idle, name=name):
                            await original(method, params)
                            if method == "fixture/idle":
                                idle.set()
                            elif method == "fixture/roundtrip-complete" and name == "alice":
                                completed[params["body"]].set()
                        adapter.rpc = RPC(process, adapter.request, observed, jsonrpc=False)
                        # Only installed-vendor startup is substituted. Actual native tool
                        # observers, dispatch, receipt/reply calls and broker remain intact.
                        async def already_started():
                            return None
                        adapter.start = already_started
                        await adapter.start_prompt("Begin idle fixture session")
                        await asyncio.wait_for(idle.wait(), 10)
                        self.assertFalse(adapter.busy, "Recipient must begin idle before controller priming")
                    live.capture(run)
                    frozen = live.load(run / "frozen.json")
                    tasks = [asyncio.create_task(adapter.run()) for adapter in adapters]
                    with patch.dict(os.environ, {"XORDINATION_ENDPOINT": endpoint}):
                        try:
                            dispatch = await asyncio.wait_for(
                                asyncio.to_thread(live.send_tasks, run, phase_timeout=phase_timeout), phase_timeout * 3 + 3)
                        except Exception as error:
                            if recipient_mode != "no-gate":
                                raise
                            dispatch = {"error": {"type": type(error).__name__, "code": getattr(error, "code", None)}}
                    if recipient_mode == "gate":
                        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in completed.values())), 10)
                        # The fixture acks a reply before its adapter records the submitted attempt.
                        await asyncio.wait_for(self.replies_submitted(run, completed), 10)
                    report = live.verify(run)
                    case = next(case for case in frozen["cases"] if case["scenario"] == "busy")
                    gate = run / "gates" / case["case_id"]
                    if mutate_wait_boundary:
                        self.assertEqual("pass", report["status"], "A genuine passing control is required")
                        witness = live.load(gate / "native-ready.json")
                        path = run / "native" / "bob.jsonl"
                        original_capture = path.read_bytes()
                        events = [json.loads(line) for line in original_capture.splitlines()]
                        selected = [event for event in events if event["call_id"] == witness["native_call_id"]]
                        self.assertEqual(1, len(selected))
                        self.assertEqual("xordination_wait", selected[0]["tool"])
                        selected[0]["monotonic_ns"] = witness["monotonic_ns"] + 1
                        lines, offset = [], 0
                        for event in events:
                            event["sequence"] = offset
                            line = live.encoded(event) + b"\n"
                            lines.append(line)
                            offset += len(line)
                        try:
                            path.write_bytes(b"".join(lines))
                            mutation = live.verify(run)
                            self.assertEqual([], mutation["errors"])
                            self.assertEqual(["missing_controlled_busy"], mutation["cases"][0]["failures"])
                            self.assertTrue(all(value for name, value in mutation["cases"][0]["phases"].items()
                                                if name != "controlled_busy"))
                        finally:
                            path.write_bytes(original_capture)
                        self.assertEqual("pass", live.verify(run)["status"])
                    artifacts = {name: live.load(gate / (name + ".json")) if (gate / (name + ".json")).exists() else None
                                 for name in ("armed", "ready", "submitted", "released", "closed", "failed")}
                    artifacts["native_events"], _, _ = live.native_events(run, frozen)
                    with live.readonly(run / "broker.sqlite3") as connection:
                        artifacts["challenge"] = [dict(row) for row in connection.execute(
                            "SELECT id,sender,sender_generation,recipient,original_generation,delivery_generation FROM messages WHERE idempotency_key=?",
                            (case["key"],))]
                        artifacts["challenge_tasks"] = [dict(row) for row in connection.execute(
                            "SELECT id FROM messages WHERE idempotency_key=?", ("acceptance-task-" + case["case_id"],))]
                        artifacts["attempts"] = [dict(row) for row in connection.execute(
                            "SELECT id,message_id,generation,native_id,state FROM attempts WHERE message_id IN (SELECT id FROM messages WHERE idempotency_key=?)",
                            (case["key"],))]
                        artifacts["prime"] = [dict(row) for row in connection.execute(
                            "SELECT id FROM messages WHERE idempotency_key=?", ("acceptance-prime-" + case["case_id"],))]
                        artifacts["replies"] = {case["scenario"]: [dict(row) for row in connection.execute(
                            "SELECT id,body FROM messages WHERE body=?", (case["reply"],))] for case in frozen["cases"]}
                    return dispatch, report, frozen, artifacts
                finally:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
                    for adapter in adapters:
                        await adapter.close()
                        if adapter.rpc and adapter.rpc.process.stderr:
                            # Capture only fixture errors if an assertion fails; pipes are
                            # owned and the process is stopped by RPC.close().
                            await adapter.rpc.process.stderr.read()

    def assert_busy_gate(self, frozen, artifacts):
        ready, submitted, released = (artifacts[name] for name in ("ready", "submitted", "released"))
        case = next(case for case in frozen["cases"] if case["scenario"] == "busy")
        for event in (ready, submitted, released):
            self.assertEqual(frozen["run_id"], event["run_id"])
            self.assertEqual(case["case_id"], event["case_id"])
            self.assertEqual("bob", event["participant"])
            self.assertEqual(frozen["bindings"]["bob"]["generation"], event["generation"])
            self.assertEqual("native-bob", event["native_id"])
        self.assertEqual(ready["native_call_id"], submitted["native_call_id"])
        self.assertLess(ready["monotonic_ns"], submitted["monotonic_ns"])
        self.assertLess(submitted["monotonic_ns"], released["monotonic_ns"])
        self.assertEqual("submitted", released["reason"])
        self.assertEqual([submitted["message_id"]], [message["id"] for message in artifacts["challenge"]])
        self.assertEqual([submitted["attempt_id"]], [attempt["id"] for attempt in artifacts["attempts"]])
        self.assertEqual("native-bob", artifacts["attempts"][0]["native_id"])
        self.assertEqual("submitted", artifacts["attempts"][0]["state"])
        self.assertTrue(any(event["tool"] == "xordination_wait" and event["call_id"] == ready["native_call_id"]
                            and event["arguments"] == artifacts["armed"]["wait_arguments"]
                            for event in artifacts["native_events"]))
        recipient_actions = [event for event in artifacts["native_events"] if event["participant"] == "bob"
                             and ((event["tool"] == "xordination_ack"
                                   and event["arguments"].get("message_id") == submitted["message_id"])
                                  or (event["tool"] == "xordination_send"
                                      and event["arguments"].get("body") == case["reply"]))]
        self.assertEqual({"xordination_ack", "xordination_send"}, {event["tool"] for event in recipient_actions})
        self.assertTrue(all(event["monotonic_ns"] > released["monotonic_ns"] for event in recipient_actions))
        self.assertEqual("released", artifacts["closed"]["reason"])
        self.assertIsNone(artifacts["failed"])

    async def test_idle_recipient_enters_native_hold_before_busy_pair_is_sent(self):
        dispatch, report, frozen, artifacts = await self.exercise_busy(mutate_wait_boundary=True)
        self.assertEqual("pass", report["status"], report)
        self.assertEqual(1, report["passed_cases"])
        self.assertEqual(1, len(frozen["cases"]))
        self.assertTrue(report["cases"][0]["phases"]["native_ack"])
        self.assertTrue(report["cases"][0]["phases"]["native_reply"])
        self.assert_busy_gate(frozen, artifacts)

    async def test_idle_roundtrip_completes_before_busy_prime_in_mixed_matrix(self):
        dispatch, report, frozen, artifacts = await self.exercise_busy(scenarios=("idle", "busy"))
        self.assertEqual("pass", report["status"], report)
        self.assertEqual(2, report["passed_cases"])
        self.assert_busy_gate(frozen, artifacts)
        idle_reply = artifacts["replies"]["idle"][0]["id"]
        prime = artifacts["prime"][0]["id"]
        events = artifacts["native_events"]
        idle_ack = next(event for event in events if event["participant"] == "alice"
                        and event["tool"] == "xordination_ack" and event["arguments"].get("message_id") == idle_reply)
        prime_ack = next(event for event in events if event["participant"] == "bob"
                         and event["tool"] == "xordination_ack" and event["arguments"].get("message_id") == prime)
        self.assertLess(idle_ack["monotonic_ns"], prime_ack["monotonic_ns"])
        self.assertLess(prime_ack["monotonic_ns"], artifacts["ready"]["monotonic_ns"])

    async def test_missing_native_readiness_is_the_intended_red(self):
        dispatch, report, frozen, artifacts = await self.exercise_busy(recipient_mode="no-gate", phase_timeout=.2)
        self.assertEqual("fail", report["status"])
        self.assertEqual(0, report["passed_cases"])
        # The controller must identify the readiness failure and refrain from
        # dispatching the pair; an unrelated setup/transport failure is not proof.
        self.assertEqual("native_readiness_timeout", artifacts["failed"]["reason"])
        self.assertEqual("readiness", artifacts["failed"]["stage"])
        self.assertIsNone(artifacts["ready"])
        self.assertIsNone(artifacts["submitted"])
        self.assertEqual([], artifacts["challenge"])
        self.assertEqual([], artifacts["challenge_tasks"])
        self.assertFalse(any(event["tool"] == "xordination_send" and event["arguments"].get("body") == frozen["cases"][0]["body"]
                             for event in artifacts["native_events"]))
        self.assertIn("required_case_unexercised", report["cases"][0]["failures"])


if __name__ == "__main__":
    unittest.main()
