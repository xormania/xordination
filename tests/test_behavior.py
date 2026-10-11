"""Calibrated behavior controls: unrelated setup failures never prove a fault."""

from copy import deepcopy
from dataclasses import FrozenInstanceError
import os
from pathlib import Path
import tempfile
import threading
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from behavior import core_scenarios, run_suite, Scenario, verify_case, verify_run
from helpers import isolated_process_env, MCPProcess, owned_broker
from xordination.store import Store, StoreError


class BehaviorTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.plan, cls.manifests, cls.report = run_suite()

    def test_all_required_directed_pairs_and_lifecycle_journeys_pass(self):
        self.assertEqual(10, len(self.report["cases"]))
        self.assertEqual({}, verify_run(self.plan, self.manifests, self.report))
        with self.assertRaises(FrozenInstanceError):
            self.plan.run_nonce = "changed"
        self.assertEqual(2, len([s for s in core_scenarios(("worker-one", "worker-two")) if s.kind == "exchange"]))

    def test_protocol_fixtures_cannot_certify_live_harnesses(self):
        failures = verify_run(self.plan, self.manifests, self.report, require_live=True)
        self.assertEqual(10, len(failures))
        self.assertTrue(all("evidence_mode" in codes for codes in failures.values()))

    def test_unexercised_and_failed_cases_preserve_verified_neighbors(self):
        report = deepcopy(self.report)
        missing = report["cases"].pop()["manifest"]["scenario"]["name"]
        first = report["cases"][0]
        first["events"]["receipt"]["generation"] = "another-session"
        name = first["manifest"]["scenario"]["name"]
        failures = verify_run(self.plan, self.manifests, report)
        self.assertEqual({missing, name}, set(failures))
        self.assertEqual(("unexercised",), failures[missing])
        self.assertIn("receipt_identity", failures[name])

    def test_cross_run_receipt_reply_and_evidence_level_are_rejected(self):
        expected = self.manifests[0]
        for mutate, code in (
            (lambda e: e["manifest"].update(run_nonce="old-run"), "manifest_identity"),
            (lambda e: e["events"]["accepted"].update(from_generation="old-generation"), "message_identity"),
            (lambda e: e["events"]["receipt"].update(message_id="another-message"), "receipt_identity"),
            (lambda e: e["events"]["receipt"].update(kind="native_transport"), "receipt_identity"),
            (lambda e: e["events"]["reply"].update(body="reply from old transcript"), "reply_link"),
        ):
            with self.subTest(code=code):
                evidence = deepcopy(self.report["cases"][0])
                mutate(evidence)
                self.assertIn(code, verify_case(expected, evidence))

    def test_duplicate_manifest_mixed_source_and_malformed_case_fail_closed(self):
        failures = verify_run(self.plan, self.manifests + (self.manifests[0],), self.report)
        self.assertIn("case_manifest_identity", failures["run"])
        report = deepcopy(self.report)
        report["final_source_sha256"] = "different-source"
        report["cases"][0]["events"] = []
        failures = verify_run(self.plan, self.manifests, report)
        self.assertIn("source_changed", failures["run"])
        self.assertEqual(("malformed_evidence",), failures[self.manifests[0].scenario.name])
        self.assertEqual(2, len(failures))
        report = deepcopy(self.report)
        report["cases"].append(report["cases"][0])
        self.assertIn("unexpected_or_duplicate_case", verify_run(self.plan, self.manifests, report)["run"])

    def test_actual_setup_failure_is_reported_and_next_case_still_executes(self):
        original = Store.join
        first = True
        def fail_first(store, *args, **kwargs):
            nonlocal first
            if first:
                first = False
                raise OSError("Planted unrelated setup error")
            return original(store, *args, **kwargs)
        scenarios = (Scenario("setup-fails", "duplicate", "alpha", "beta"),
                     Scenario("healthy", "duplicate", "beta", "alpha"))
        with patch.object(Store, "join", fail_first):
            plan, manifests, report = run_suite(scenarios=scenarios)
        self.assertEqual(2, len(report["cases"]))
        self.assertEqual({"setup-fails": ("execution_error",)}, verify_run(plan, manifests, report))

    def test_production_mutations_trigger_the_intended_assertions(self):
        original_ack, original_send = Store.ack, Store.send
        original_recovery, original_finish = Store.recover_claims, Store.finish
        def lose_receipt(store, ctx, message_id):
            result = original_ack(store, ctx, message_id)
            with store._transaction():
                store._db.execute("DELETE FROM receipts WHERE id=?", (result["receipt"]["id"],))
            return result
        def unlink_reply(store, ctx, to, body, key, **arguments):
            arguments.pop("reply_to", None)
            return original_send(store, ctx, to, body, key, **arguments)
        sends = 0
        def duplicate_message(store, ctx, to, body, key, **arguments):
            nonlocal sends
            if key == "challenge" and body != "different intent":
                sends += 1
                if sends == 2:
                    key = "fault-bypassed-idempotency"
            return original_send(store, ctx, to, body, key, **arguments)
        def automatic_replay(store):
            original_recovery(store)
            with store._transaction():
                store._db.execute("UPDATE messages SET state='queued' WHERE state='uncertain'")
        def accept_stale_finish(store, ctx, message_id, attempt_id, outcome, detail=None):
            try:
                return original_finish(store, ctx, message_id, attempt_id, outcome, detail)
            except StoreError as error:
                if error.code != "stale_attempt":
                    raise
                with store._transaction():
                    store._db.execute("UPDATE messages SET state=? WHERE id=?", (outcome, message_id))
                return {"message_id": message_id, "attempt_id": attempt_id, "state": outcome}
        catalog = (
            ("duplicate", "ack", lose_receipt, "receipt_persisted"),
            ("duplicate", "send", unlink_reply, "reply_link"),
            ("duplicate", "send", duplicate_message, "duplicate_message"),
            ("restart_retry", "recover_claims", automatic_replay, "automatic_replay_blocked"),
            ("replacement", "finish", accept_stale_finish, "stale_callback_fenced"),
        )
        for kind, method, mutation, intended in catalog:
            with self.subTest(fault=intended):
                scenarios = (Scenario("calibration", kind, "alpha", "beta"),)
                plan, manifests, control = run_suite(scenarios=scenarios)
                self.assertEqual({}, verify_run(plan, manifests, control), "Passing control is required")
                with patch.object(Store, method, mutation):
                    plan, manifests, candidate = run_suite(scenarios=scenarios)
                failures = verify_run(plan, manifests, candidate)
                self.assertIn(intended, failures.get("calibration", ()))
                self.assertNotIn("execution_error", failures["calibration"])
                self.assertNotIn("malformed_evidence", failures["calibration"])

    def test_shared_subprocess_environment_is_fresh_and_retains_explicit_measurement(self):
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ, {"OPENAI_API_KEY": "ambient-secret", "ANTHROPIC_API_KEY": "ambient-secret",
                                         "XORDINATION_TOKEN": "ambient-secret", "PYTHONPATH": "/ambient/imports"}, clear=True):
                env = isolated_process_env(directory, token="fixture-owned-token")
                self.assertEqual(directory, env["HOME"])
                self.assertEqual("fixture-owned-token", env["XORDINATION_TEST_TOKEN"])
                self.assertFalse(any(name in env for name in ("OPENAI_API_KEY", "ANTHROPIC_API_KEY", "XORDINATION_TOKEN")))
                self.assertNotIn("/ambient/imports", env["PYTHONPATH"])
                with patch.dict(os.environ, {"COVERAGE_PROCESS_START": "/explicit/measurement.ini"}), patch.dict(
                    "sys.modules", {"coverage": SimpleNamespace(__file__="/explicit/site-packages/coverage/__init__.py")}):
                    measured = isolated_process_env(directory)
                    self.assertEqual("/explicit/measurement.ini", measured["COVERAGE_PROCESS_START"])
                    self.assertIn("/explicit/site-packages", measured["PYTHONPATH"])
                    self.assertIn(str(Path(__file__).resolve().parents[1] / "scripts" / "ci"), measured["PYTHONPATH"])
                    self.assertNotIn("/ambient/imports", measured["PYTHONPATH"])

    def test_shared_stdio_client_correlates_calls_while_a_wait_is_held(self):
        with tempfile.TemporaryDirectory() as directory:
            with owned_broker(Path(directory) / "broker.sqlite3") as (store, server, endpoint):
                store.create_space("wait")
                binding = store.join("wait", "worker")
                entered, release = threading.Event(), threading.Event()
                original = server.dispatch
                def held(token, operation, arguments):
                    if operation == "wait":
                        entered.set()
                        if not release.wait(3):
                            raise TimeoutError("Owned wait gate was not released")
                    return original(token, operation, arguments)
                with patch.object(server, "dispatch", held):
                    with MCPProcess(endpoint, binding["token"], directory) as client:
                        try:
                            waiting = client.start_request("tools/call", {"name": "xordination_wait", "arguments": {"timeout": 0}})
                            self.assertTrue(entered.wait(2), "Wait did not enter its HTTP boundary")
                            self.assertEqual(["worker"], [p["participant"] for p in client.tool("peers")["peers"]])
                            release.set()
                            response = client.response(waiting)
                            self.assertFalse(response["isError"])
                            self.assertEqual([], response["structuredContent"]["messages"])
                        finally:
                            release.set()


if __name__ == "__main__":
    unittest.main()
