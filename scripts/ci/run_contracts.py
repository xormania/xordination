"""Run the selected unittest modules as one suite, rejecting zero-test skips."""

import argparse
import json
from pathlib import Path
import sys
import unittest

from select_checks import GROUPS


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
        def cases(item):
            if isinstance(item, unittest.TestSuite):
                for child in item:
                    yield from cases(child)
            else:
                yield item
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
    args = parser.parse_args(argv)
    selection = json.loads(Path(args.selection).read_text())
    print("Selected test groups: " + ", ".join(selection["groups"]), flush=True)
    result = unittest.TextTestRunner(verbosity=2).run(load_suite(selection))
    return 0 if result.wasSuccessful() else 1


if __name__ == "__main__":
    raise SystemExit(main())

