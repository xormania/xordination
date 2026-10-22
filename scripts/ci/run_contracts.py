"""Run selected contracts and fail when required cases are skipped or unexercised."""

import argparse
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest

from select_checks import FAST_GROUPS, GROUPS, SHARDS

FAST_MODULES = {"test_" + name for name in FAST_GROUPS}


def cases(item):
    if isinstance(item, unittest.TestSuite):
        for child in item:
            yield from cases(child)
    else:
        yield item


class CensusResult(unittest.TextTestResult):
    """Track actual case starts independently from collection and suite fixtures."""

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.started_ids = []

    def startTest(self, test):
        self.started_ids.append(test.id())
        super().startTest(test)


def load_suite(selection):
    names = selection.get("groups")
    if not isinstance(names, list) or not names or len(names) != len(set(names)) or any(name not in GROUPS for name in names):
        raise ValueError("Selection must contain unique known test groups")
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    for name in names:
        path = Path("tests/test_" + name + ".py")
        if not path.is_file():
            raise ValueError("Selected test module is absent: " + str(path))
    if set(names) == set(GROUPS):
        # Unknown/new test modules trigger full classification and must be included.
        suite = loader.discover("tests")
        if not suite.countTestCases():
            raise ValueError("Full test discovery contains no tests")
        if loader.errors:
            raise ValueError("Test modules failed discovery: " + "; ".join(loader.errors))
        modules = {case.__class__.__module__ for case in cases(suite)}
        for name in names:
            if "test_" + name not in modules:
                raise ValueError("Selected test module contains no tests: test_" + name)
        return suite
    for name in names:
        path = Path("tests/test_" + name + ".py")
        group = loader.discover("tests", pattern=path.name)
        if not group.countTestCases():
            raise ValueError("Selected test module contains no tests: " + str(path))
        suite.addTests(group)
    if loader.errors:
        raise ValueError("Selected test modules failed discovery: " + "; ".join(loader.errors))
    return suite


def in_tier(module, tier):
    return tier == "all" or (module in FAST_MODULES) == (tier == "fast")


def keep(suite, predicate):
    kept = unittest.TestSuite()
    kept.addTests(case for case in cases(suite) if predicate(case.__class__.__module__))
    return kept


def shard_modules(modules):
    """Partition modules into the fixed shards; unknown modules join the last shard."""
    index = {"test_" + name: number for number, shard in enumerate(SHARDS) for name in shard}
    buckets = [[] for _ in SHARDS]
    for module in sorted(modules):
        buckets[index.get(module, len(SHARDS) - 1)].append(module)
    return [bucket for bucket in buckets if bucket]


def run_inline(suite):
    started = time.monotonic()
    result = unittest.TextTestRunner(verbosity=2, resultclass=CensusResult).run(suite)
    return {
        "started_ids": result.started_ids,
        "skipped": [{"test": test.id(), "reason": reason} for test, reason in result.skipped],
        "failures": [test.id() for test, _ in result.failures],
        "errors": [test.id() for test, _ in result.errors],
        "expected_failures": [test.id() for test, _ in result.expectedFailures],
        "unexpected_successes": [test.id() for test in result.unexpectedSuccesses],
        "successful": result.wasSuccessful(),
        "duration_seconds": round(time.monotonic() - started, 6),
    }


def run_shards(selection_path, tier, buckets):
    """Run shards as parallel process groups; the first failure stops the rest."""
    outcome = {"started_ids": [], "skipped": [], "failures": [], "errors": [], "expected_failures": [],
               "unexpected_successes": [], "successful": True, "shards": []}
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix="xordination-shards-") as folder:
        running = []
        for number, modules in enumerate(buckets):
            log = open(Path(folder) / f"{number}.log", "w+", encoding="utf-8")
            report = Path(folder) / f"{number}.json"
            process = subprocess.Popen(
                [sys.executable, str(Path(__file__).resolve()), "--selection", str(selection_path),
                 "--tier", tier, "--modules", ",".join(modules), "--report", str(report)],
                stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
            running.append((number, modules, process, log, report, time.monotonic()))
        stopped = False
        while running:
            for item in list(running):
                number, modules, process, log, report, began = item
                code = process.poll()
                if code is None and not stopped:
                    continue
                if code is None:
                    # Shards own their process groups, so brokers and fixtures they start stop too.
                    try:
                        os.killpg(process.pid, signal.SIGTERM)
                    except ProcessLookupError:
                        pass
                    try:
                        code = process.wait(10)
                    except subprocess.TimeoutExpired:
                        os.killpg(process.pid, signal.SIGKILL)
                        code = process.wait()
                    note = "stopped after an earlier shard failed"
                else:
                    note = "passed" if code == 0 else "failed"
                running.remove(item)
                log.seek(0)
                print(f"::group::Shard {number + 1}/{len(buckets)} ({', '.join(modules)}): {note}", flush=True)
                print(log.read(), end="", flush=True)
                print("::endgroup::", flush=True)
                log.close()
                part = json.loads(report.read_text(encoding="utf-8")) if report.is_file() else {}
                for key in ("started_ids", "skipped", "failures", "errors", "expected_failures", "unexpected_successes"):
                    outcome[key] += part.get(key, [])
                outcome["shards"].append({"shard": number + 1, "modules": modules, "exit_code": code, "result": note,
                                          "duration_seconds": round(time.monotonic() - began, 3)})
                if code != 0:
                    outcome["successful"] = False
                    if not stopped and note == "failed":
                        print(f"Shard {number + 1} failed; stopping the remaining shards.", flush=True)
                    stopped = True
            time.sleep(.05)
    # Shards finish in any order; the census lists them in their fixed order.
    outcome["shards"].sort(key=lambda shard: shard["shard"])
    outcome["duration_seconds"] = round(time.monotonic() - started, 6)
    return outcome


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", required=True)
    parser.add_argument("--report", type=Path, help="write the execution census even when contracts fail")
    parser.add_argument("--tier", choices=("all", "fast", "slow"), default="all",
                        help="fast: in-process groups that gate the rest; slow: every other selected case")
    parser.add_argument("--parallel", action="store_true", help="run the selected cases as parallel shards")
    parser.add_argument("--modules", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    selection = json.loads(Path(args.selection).read_text())
    print("Selected test groups: " + ", ".join(selection["groups"]) + " (tier: " + args.tier + ")", flush=True)
    suite = keep(load_suite(selection), lambda module: in_tier(module, args.tier))
    if args.modules is not None:
        suite = keep(suite, set(args.modules.split(",")).__contains__)
    collected = [case.id() for case in cases(suite)]
    if len(collected) != len(set(collected)):
        raise ValueError("Duplicate test identities cannot establish required execution")
    modules = {case.__class__.__module__ for case in cases(suite)}
    buckets = shard_modules(modules)
    if args.parallel and len(buckets) > 1:
        outcome = run_shards(Path(args.selection).resolve(), args.tier, buckets)
    else:
        outcome = run_inline(suite)
    started_ids = outcome.pop("started_ids")
    not_run = sorted(set(collected) - set(started_ids))
    skipped = outcome["skipped"]
    report = {
        "schema": 1,
        "groups": selection["groups"],
        "tier": args.tier,
        "full": set(selection["groups"]) == set(GROUPS),
        "collected": len(collected),
        "started": len(started_ids),
        "executed": len(started_ids) - len({item["test"] for item in skipped if item["test"] in collected}),
        "skipped": skipped,
        "not_run": not_run,
        "failures": outcome["failures"],
        "errors": outcome["errors"],
        "expected_failures": outcome["expected_failures"],
        "unexpected_successes": outcome["unexpected_successes"],
        "duration_seconds": outcome["duration_seconds"],
        "required_exercised": not skipped and not not_run,
    }
    if "shards" in outcome:
        report["shards"] = outcome["shards"]
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        # Shard children also hand their started IDs to the parent's census.
        payload = dict(report, started_ids=started_ids) if args.modules is not None else report
        args.report.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print("Execution census: " + json.dumps(report, sort_keys=True), flush=True)
    if skipped or not_run or outcome["expected_failures"]:
        print("Required contracts were skipped, not exercised, or marked expected failures.", file=sys.stderr)
    return 0 if outcome["successful"] and report["required_exercised"] and not outcome["expected_failures"] else 1


if __name__ == "__main__":
    raise SystemExit(main())

