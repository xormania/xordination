"""Run selected contracts and fail when required cases are skipped or unexercised."""

import argparse
import json
from pathlib import Path
import sys
import time
import unittest

from select_checks import GROUPS


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


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", required=True)
    parser.add_argument("--report", type=Path, help="write the execution census even when contracts fail")
    args = parser.parse_args(argv)
    selection = json.loads(Path(args.selection).read_text())
    print("Selected test groups: " + ", ".join(selection["groups"]), flush=True)
    suite = load_suite(selection)
    collected = [case.id() for case in cases(suite)]
    if len(collected) != len(set(collected)):
        raise ValueError("Duplicate test identities cannot establish required execution")
    started = time.monotonic()
    result = unittest.TextTestRunner(verbosity=2, resultclass=CensusResult).run(suite)
    not_run = sorted(set(collected) - set(result.started_ids))
    skipped = [{"test": test.id(), "reason": reason} for test, reason in result.skipped]
    report = {
        "schema": 1,
        "groups": selection["groups"],
        "full": set(selection["groups"]) == set(GROUPS),
        "collected": len(collected),
        "started": result.testsRun,
        "executed": result.testsRun - len({item["test"] for item in skipped if item["test"] in collected}),
        "skipped": skipped,
        "not_run": not_run,
        "failures": [test.id() for test, _ in result.failures],
        "errors": [test.id() for test, _ in result.errors],
        "expected_failures": [test.id() for test, _ in result.expectedFailures],
        "unexpected_successes": [test.id() for test in result.unexpectedSuccesses],
        "duration_seconds": round(time.monotonic() - started, 6),
        "required_exercised": not skipped and not not_run,
    }
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print("Execution census: " + json.dumps(report, sort_keys=True), flush=True)
    if skipped or not_run or result.expectedFailures:
        print("Required contracts were skipped, not exercised, or marked expected failures.", file=sys.stderr)
    return 0 if result.wasSuccessful() and report["required_exercised"] and not result.expectedFailures else 1


if __name__ == "__main__":
    raise SystemExit(main())

