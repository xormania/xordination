"""Check measurement boundaries and correctness, without timing thresholds."""

import copy
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SPEC = importlib.util.spec_from_file_location("xordination_benchmark", ROOT / "scripts/testing/benchmark.py")
benchmark = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = benchmark
SPEC.loader.exec_module(benchmark)


class PerformanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # Six fresh trials, four retained, plus one separate broker process;
        # small real HTTP workload shared only as immutable evidence for
        # report-format tests.
        cls.config = benchmark.Config(messages=2, workers=2, trials=2, warmup=1,
                                      process_trials=1, idle_ms=50)
        with patch.object(benchmark, "trial", wraps=benchmark.trial) as measured:
            cls.report = benchmark.run(cls.config, session_id="paired-test-session")
            cls.executed_trials = measured.call_count

    def test_warmup_is_executed_but_excluded_from_measured_samples(self):
        report = self.report
        self.assertEqual(6, self.executed_trials)
        self.assertEqual(2, report["warmup_discarded"])
        self.assertEqual(4, len(report["trials"]))
        self.assertEqual(4, report["summary"]["serial_roundtrip"]["latency_ms"]["roundtrip"]["count"])
        self.assertEqual(8, report["summary"]["concurrent_backlog"]["latency_ms"]["send"]["count"])
        self.assertEqual("report_only", report["performance_gate"])
        self.assertEqual("verified", report["status"])
        for trial in report["trials"]:
            self.assertEqual(0, trial["pending_after_work"])
            self.assertGreater(trial["storage_bytes"]["after_close"], 0)
            self.assertGreaterEqual(trial["storage_bytes"]["after_work"],
                                    trial["storage_bytes"]["after_setup"])
        self.assertNotEqual(report["summary"]["serial_roundtrip"]["work_unit"],
                            report["summary"]["concurrent_backlog"]["work_unit"])

    def test_broker_process_reports_resources_wait_capacity_and_restart(self):
        process = self.report["broker_process"]
        limits = self.report["manifest"]["broker_limits"]
        self.assertEqual({"max_requests": 64, "max_waits": 16}, limits)
        self.assertEqual(1, len(process["trials"]))
        trial = process["trials"][0]
        self.assertEqual("verified", trial["correctness"])
        waits = trial["wait_capacity"]
        self.assertEqual(16, waits["waiting_clients"])
        self.assertEqual(self.config.wait_overflow, waits["overflow_backpressure"])
        self.assertTrue(waits["capacity_recovered"])
        self.assertEqual(16, process["summary"]["wait_ms"]["wake"]["count"])
        self.assertEqual(3, process["summary"]["wait_ms"]["saturated_inbox"]["count"])
        self.assertEqual(4, trial["loaded_work_units"])
        restart = trial["restart"]
        self.assertGreater(restart["first_success_ms"], 0)
        self.assertGreaterEqual(restart["first_success_attempts"], 1)
        self.assertEqual((1, 1), (restart["retained_pending_messages"], restart["retained_receipts"]))
        # /proc is optional; where present every window reports the broker child.
        linux = Path("/proc/self/stat").is_file()
        self.assertEqual("/proc" if linux else None, trial["resource_source"])
        for window in ("idle", "loaded", "waiting"):
            if linux:
                self.assertGreater(trial[window]["rss_bytes"], 0)
                self.assertGreaterEqual(trial[window]["cpu_percent"], 0)
                self.assertIn(window, process["summary"])
            else:
                self.assertIsNone(trial[window])
        self.assertNotIn("broker process RSS", self.report["manifest"]["unmeasured"])

    def test_wait_measurement_refuses_a_limit_below_the_documented_capacity(self):
        with patch.object(sys, "path", sys.path[:]):
            product = benchmark.runtime(ROOT / "src")
        config = benchmark.Config(wait_overflow=1)
        for actual, claimed in ((2, 2), (2, 3)):
            names = [f"waiter-{n}" for n in range(claimed)] + ["overflow-0", "waker", "probe"]
            with self.subTest(actual=actual, claimed=claimed), tempfile.TemporaryDirectory() as temporary, \
                    product.owned_broker(Path(temporary) / "state.sqlite3", max_waits=actual) as (store, _, endpoint):
                store.create_space("benchmark")
                bindings = {name: store.join("benchmark", name) for name in names}
                clients = {name: product.Client(endpoint, bindings[name]["token"]) for name in names}
                arguments = (config, clients, product.BrokerError, bindings, claimed, "body")
                if actual == claimed:
                    result = benchmark.wait_capacity(*arguments, hold_deadline_s=5)
                    self.assertEqual((2, 1), (result["waiting_clients"], result["overflow_backpressure"]))
                else:
                    # A waiter queued behind the real limit is not a held wait.
                    with self.assertRaisesRegex(AssertionError, "not held at the documented limit"):
                        benchmark.wait_capacity(*arguments, hold_deadline_s=5)

    def test_statistics_use_nearest_rank_and_reject_invalid_samples(self):
        self.assertEqual({"count": 5, "min": 1, "max": 5, "p50": 3, "p95": 5},
                         benchmark.stats([5, 3, 1, 4, 2]))
        for samples in ([], [float("nan")], [float("inf")], [-1], [True], [1, None]):
            with self.subTest(samples=samples), self.assertRaises(ValueError):
                benchmark.stats(samples)

    def test_invalid_configuration_is_refused_before_running_product(self):
        for name, value in (("trials", 0), ("warmup", -1), ("messages", 1001),
                            ("workers", 17), ("payload_bytes", 65537), ("seed", -1),
                            ("trials", True), ("process_trials", 0), ("idle_ms", -1),
                            ("wait_overflow", 17)):
            config = benchmark.Config(**{name: value})
            with self.subTest(name=name, value=value), patch.object(benchmark, "runtime") as product:
                with self.assertRaises(ValueError):
                    benchmark.run(config)
                product.assert_not_called()

    def test_comparison_reports_variation_and_refuses_different_measurements(self):
        head = copy.deepcopy(self.report)
        head["manifest"]["product"]["sha256"] = "different-product"
        before = self.report["summary"]["serial_roundtrip"]["latency_ms"]["send"]["p95"]
        head["summary"]["serial_roundtrip"]["latency_ms"]["send"]["p95"] = before * 2
        comparison = benchmark.compare(self.report, head)
        self.assertAlmostEqual(100, comparison["scenarios"]["serial_roundtrip"]["latency"]["send.p95"]["change_percent"])
        self.assertEqual("report_only", comparison["performance_gate"])
        for field in ("config", "driver", "environment", "scope", "measurement_session_id"):
            bad = copy.deepcopy(head)
            bad["manifest"][field] = "different"
            with self.subTest(field=field), self.assertRaises(ValueError):
                benchmark.compare(self.report, bad)
        process = comparison["broker_process"]
        self.assertIn("restart_ms.first_success.p50", process)
        self.assertIn("wait_ms.wake.p95", process)
        # Reports from before process measurements still compare their scenarios.
        older = copy.deepcopy(head)
        del older["broker_process"]
        for field in ("broker_process_scope", "broker_limits"):
            del older["manifest"][field]
        comparison = benchmark.compare(self.report, older)
        self.assertEqual(({}, "unavailable"), (comparison["broker_process"],
                                               comparison["broker_process_comparison"]))
        head["status"] = "failed"
        with self.assertRaises(ValueError):
            benchmark.compare(self.report, head)

    def test_missing_receipt_is_fatal_even_without_performance_threshold(self):
        class IncorrectRecipient:
            def call(self, unused_operation, **unused_arguments):
                return {"message_id": "message", "acknowledged": False,
                        "receipt": {"id": "receipt", "message_id": "message", "observer": "bob",
                                    "generation": "generation", "kind": "client_ack"}}
        with self.assertRaisesRegex(AssertionError, "receipt"):
            benchmark.acknowledge(benchmark.Calls(), IncorrectRecipient(),
                                  {"participant": "bob", "generation": "generation"}, "message")

    def test_explicit_source_runs_selected_bytes_despite_ambient_checkout_path(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / "selected-src"
            shutil.copytree(ROOT / "src", source, ignore=shutil.ignore_patterns("__pycache__"))
            product = source / "xordination" / "__init__.py"
            product.write_text(product.read_text() + "\n# Selected benchmark source.\n")
            output = Path(temporary) / "report.json"
            env = dict(os.environ, PYTHONPATH=str(ROOT / "src"))
            completed = subprocess.run(
                [sys.executable, str(ROOT / "scripts/testing/benchmark.py"), "--source-root", str(source),
                 "--messages", "1", "--workers", "1", "--warmup", "0", "--trials", "1",
                 "--process-trials", "1", "--idle-ms", "0", "--output", str(output)], cwd=temporary, env=env,
                capture_output=True, text=True, timeout=20)
            self.assertEqual(0, completed.returncode, completed.stderr)
            report = json.loads(output.read_text())
            self.assertNotEqual(self.report["manifest"]["product"]["sha256"],
                                report["manifest"]["product"]["sha256"])
            self.assertTrue(all(Path(path).is_relative_to(source)
                                for path in report["manifest"]["loaded_product_modules"].values()))
            # Already-loaded product modules cannot be silently reused when a
            # caller asks this interpreter to benchmark another source tree.
            with patch.object(sys, "path", sys.path[:]), self.assertRaisesRegex(ValueError, "escaped"):
                benchmark.runtime(source)

    def test_cli_writes_failed_evidence_for_invalid_source_or_configuration(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary) / "failed.json"
            with patch("sys.stderr"):
                status = benchmark.main(["--output", str(output), "--messages", "0"])
            self.assertEqual(1, status)
            self.assertEqual("failed", json.loads(output.read_text())["status"])
            with patch("sys.stderr"):
                status = benchmark.main(["--output", str(output), "--compare-base", str(output)])
            self.assertEqual(1, status)
            self.assertIn("Both", json.loads(output.read_text())["message"])


if __name__ == "__main__":
    unittest.main()
