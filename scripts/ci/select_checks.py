"""Select complete behavioral groups from the tested Git change, defaulting full."""

import argparse
import json
import os
from pathlib import Path
import re
import subprocess


GROUPS = ("store", "adapters", "mcp", "server", "cli", "end_to_end", "ci_selection",
          "behavior", "performance", "live_acceptance", "live_native_busy", "live_tmux")
# In-process contracts that finish in seconds run first; slower groups start only after they pass.
FAST_GROUPS = ("store", "mcp", "server", "cli", "ci_selection")
# Slow groups run as parallel shard processes balanced by measured duration. Test
# modules outside GROUPS (only selected by a full run) join the last shard.
SHARDS = (("adapters",), ("live_tmux", "live_native_busy"),
          ("live_acceptance", "behavior", "end_to_end", "performance"))
SOURCE_GROUPS = {
    "store.py": ("store", "adapters", "server", "cli", "end_to_end", "behavior", "performance", "live_acceptance", "live_native_busy", "live_tmux"),
    "adapters.py": ("adapters", "cli", "behavior", "live_acceptance", "live_native_busy", "live_tmux"),
    "rpc.py": ("adapters", "behavior", "live_acceptance", "live_native_busy", "live_tmux"),
    # Native adapters consume TOOLS and INSTRUCTIONS through lazy imports.
    "mcp.py": ("adapters", "mcp", "cli", "end_to_end", "behavior", "live_acceptance", "live_native_busy", "live_tmux"),
    # Adapter reconnect contracts restart a real broker under a running adapter.
    "server.py": ("adapters", "server", "cli", "end_to_end", "behavior", "performance", "live_acceptance", "live_native_busy", "live_tmux"),
    "client.py": ("adapters", "mcp", "server", "cli", "end_to_end", "behavior", "performance", "live_acceptance", "live_native_busy", "live_tmux"),
    # Public dispatch and subprocess entry points are explicit dependency edges.
    # The benchmark runs a separate broker process through cli.serve.
    "cli.py": ("adapters", "mcp", "server", "cli", "end_to_end", "behavior", "performance", "live_acceptance", "live_native_busy", "live_tmux"),
    # Traces are written by the MCP frontend and native RPC and bundled by the CLI.
    "trace.py": ("adapters", "mcp", "server", "cli", "end_to_end", "behavior", "live_acceptance", "live_native_busy", "live_tmux"),
    # Lazily imported by CLI dispatch; end_to_end runs it in a separate client process.
    # It consumes the live.py verifier report; live_acceptance feeds a retained one through it.
    # live.py lazily imports its source fingerprint from here, so every live group depends on it.
    "readiness.py": ("cli", "end_to_end", "live_acceptance", "live_native_busy", "live_tmux"),
    "__init__.py": GROUPS,
    "__main__.py": GROUPS,
}
TEST_GROUPS = {"tests/test_" + name + ".py": (name,) for name in GROUPS}
TEST_GROUPS["tests/live_native_fixture.py"] = ("live_native_busy",)
SCRIPT_GROUPS = {
    "scripts/testing/benchmark.py": ("performance",),
    # The managed launcher imports live.py and starts the broker through the public CLI.
    # live.py produces the verifier report that `xordination readiness` consumes; the
    # live_acceptance group carries that producer -> consumer integration case.
    "scripts/testing/live.py": ("live_acceptance", "live_native_busy", "live_tmux"),
    "scripts/testing/live_tmux.py": ("live_tmux",),
}
# MANIFEST.in includes tests, docs and scripts by pattern, so editing their content cannot
# change the archive; check_package still verifies every one is packaged when it runs.
BUILD_INPUTS = {"pyproject.toml"}
# Inputs no build, image or contract reads; their own workflow or the lint job covers them.
INERT_INPUTS = {".gitignore", ".github/workflows/codeql.yml"}
IMAGE_PACKAGE_INPUTS = {"README.md", "LICENSE", "NOTICE", "Dockerfile"}


def full_selection(reason, paths=()):
    return {"groups": list(GROUPS), "package": True, "image": True, "benchmark": True,
            "full": True, "paths": sorted(set(paths)), "reasons": [reason]}


def classify_paths(paths):
    """Union known dependencies; unrecognized paths may never silently skip work."""
    paths = sorted(set(paths))
    groups = set()
    package = image = benchmark = False
    reasons = []
    for path in paths:
        if path.startswith("src/xordination/"):
            name = path[len("src/xordination/"):]
            if name not in SOURCE_GROUPS:
                return full_selection("Unknown runtime input: " + path, paths)
            groups.update(SOURCE_GROUPS[name])
            package = image = True
            benchmark = benchmark or "performance" in SOURCE_GROUPS[name]
            reasons.append("Runtime input: " + path)
        elif path in TEST_GROUPS:
            groups.update(TEST_GROUPS[path])
            reasons.append("Test input: " + path)
        elif path in SCRIPT_GROUPS:
            groups.update(SCRIPT_GROUPS[path])
            # The benchmark comparison runs the head driver against both product sources.
            benchmark = benchmark or path == "scripts/testing/benchmark.py"
            reasons.append("Testing driver input: " + path)
        elif path.startswith("tests/"):
            return full_selection("New or shared test input: " + path, paths)
        elif path in BUILD_INPUTS:
            # Project metadata changes interpreters, entry points and the installed package.
            groups.update(GROUPS)
            package = image = benchmark = True
            reasons.append("Build input: " + path)
        elif path == "MANIFEST.in":
            # Contracts run from the checkout; only the source archive can change.
            package = True
            reasons.append("Source-archive input: " + path)
        elif path in INERT_INPUTS:
            reasons.append("Input with no build, image or contract effect: " + path)
        elif path in IMAGE_PACKAGE_INPUTS:
            package = image = True
            reasons.append("Package/image input: " + path)
        elif path == ".dockerignore":
            # The image smoke's cross-profile client installs the package artifact.
            package = image = True
            reasons.append("Image context input: " + path)
        elif (path.startswith("docs/") and path.endswith(".md")) or path == "AGENTS.md":
            reasons.append("Documentation input: " + path)
        elif path.startswith("scripts/ci/") or path.startswith(".github/workflows/"):
            return full_selection("CI selection/execution input: " + path, paths)
        else:
            return full_selection("Unknown input: " + path, paths)
    return {"groups": [name for name in GROUPS if name in groups], "package": package,
            "benchmark": benchmark,
            "image": image, "full": groups == set(GROUPS), "paths": paths,
            "reasons": reasons or ["Complete tested diff is empty"]}


def parse_name_status(wire):
    """Parse Git's NUL records, retaining deletion and both rename/copy paths."""
    if not wire:
        return []
    fields = wire.decode("utf-8", "surrogateescape").split("\0")
    if fields.pop() != "":
        raise ValueError("Git diff did not terminate its NUL records")
    paths = []
    while fields:
        status = fields.pop(0)
        if not re.fullmatch(r"[ACDMRT](?:\d{1,3})?", status):
            raise ValueError("Unsupported or incomplete Git diff status")
        count = 2 if status[0] in "RC" else 1
        if len(fields) < count:
            raise ValueError("Incomplete Git diff path record")
        for _ in range(count):
            path = fields.pop(0)
            if not path:
                raise ValueError("Empty Git diff path")
            paths.append(path)
    return paths


def changed_paths(repo, event, payload, head):
    if event == "pull_request":
        base = payload["pull_request"]["base"]["sha"]
    elif event == "push":
        base = payload["before"]
    else:
        raise ValueError("Manual or unrecognized event requires full validation")
    for value in (base, head):
        if not isinstance(value, str) or not re.fullmatch(r"[0-9a-fA-F]{40,64}", value) or set(value) == {"0"}:
            raise ValueError("Tested Git range is unavailable")
        subprocess.run(["git", "cat-file", "-e", value + "^{commit}"], cwd=repo,
                       check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    if event == "pull_request":
        # Classify the complete base-to-tested-merge tree, not only the latest commit.
        subprocess.run(["git", "merge-base", "--is-ancestor", base, head], cwd=repo,
                       check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    result = subprocess.run(["git", "diff", "--name-status", "-z", "--no-ext-diff", "-M", base, head],
                            cwd=repo, check=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, timeout=30)
    return parse_name_status(result.stdout)


def select_change(repo, event, payload, head):
    try:
        return classify_paths(changed_paths(repo, event, payload, head))
    except (KeyError, TypeError, ValueError, OSError, subprocess.SubprocessError) as error:
        return full_selection("Complete change classification unavailable: " + type(error).__name__)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", default=os.environ.get("GITHUB_EVENT_NAME", "workflow_dispatch"))
    parser.add_argument("--event-file", default=os.environ.get("GITHUB_EVENT_PATH"))
    parser.add_argument("--head", default=os.environ.get("GITHUB_SHA"))
    parser.add_argument("--repo", default=".")
    parser.add_argument("--selection-file", required=True)
    parser.add_argument("--github-output", default=os.environ.get("GITHUB_OUTPUT"))
    args = parser.parse_args(argv)
    try:
        payload = json.loads(Path(args.event_file).read_text()) if args.event_file else {}
    except (OSError, TypeError, ValueError):
        result = full_selection("Event payload unavailable")
    else:
        result = select_change(args.repo, args.event, payload, args.head)
    # Failure to publish the selection fails the job; missing output is never a skip.
    Path(args.selection_file).write_text(json.dumps(result, indent=2) + "\n")
    if args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as stream:
            fast = any(name in FAST_GROUPS for name in result["groups"])
            slow = any(name not in FAST_GROUPS for name in result["groups"])
            for name, value in (("contracts", bool(result["groups"])),
                                ("fast", fast), ("slow", slow),
                                ("package", result["package"]), ("image", result["image"]),
                                ("benchmark", result["benchmark"]),
                                ("full", result["full"])):
                stream.write(name + "=" + str(value).lower() + "\n")
            # Later jobs restore the exact selection from this single-line output.
            stream.write("selection=" + json.dumps(result, separators=(",", ":")) + "\n")
    print(json.dumps(result, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

