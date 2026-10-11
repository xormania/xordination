"""Measure base/head product source with one frozen driver on the same CI runner."""

import argparse
import io
import json
import os
from pathlib import Path, PurePosixPath
import re
import subprocess
import sys
import tarfile
import tempfile
import uuid


ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG = {"messages": 8, "trials": 3, "workers": 4}
LIMITS = {"messages": (1, 128), "trials": (2, 12), "workers": (1, 16)}


def benchmark_config(event, payload):
    config = dict(DEFAULT_CONFIG)
    if event == "workflow_dispatch":
        inputs = payload.get("inputs", {})
        if not isinstance(inputs, dict):
            raise ValueError("Manual benchmark inputs must be an object")
        for name, (minimum, maximum) in LIMITS.items():
            value = inputs.get("benchmark_" + name, config[name])
            if isinstance(value, bool) or not re.fullmatch(r"[0-9]+", str(value)):
                raise ValueError("Invalid benchmark " + name)
            value = int(value)
            if not minimum <= value <= maximum:
                raise ValueError("Benchmark " + name + " is outside validated bounds")
            config[name] = value
    return config


def baseline_ref(event, payload):
    if event == "pull_request":
        value = payload.get("pull_request", {}).get("base", {}).get("sha")
    elif event == "push":
        value = payload.get("before")
    elif event == "workflow_dispatch":
        value = payload.get("inputs", {}).get("benchmark_base")
    else:
        value = None
    if value in (None, "") or isinstance(value, str) and set(value) == {"0"}:
        return None
    if not isinstance(value, str) or not re.fullmatch(r"[0-9a-fA-F]{40,64}", value):
        raise ValueError("Benchmark baseline must be a complete commit SHA")
    return value


def extract_source(repo, revision, target):
    """Read only the base's product bytes; never execute its benchmark/CI scripts."""
    subprocess.run(["git", "cat-file", "-e", revision + "^{commit}"], cwd=repo,
                   check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    result = subprocess.run(["git", "archive", revision, "src/xordination"], cwd=repo,
                            check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30)
    with tarfile.open(fileobj=io.BytesIO(result.stdout)) as archive:
        for member in archive:
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts or path.parts[0] != "src":
                raise ValueError("Unsafe benchmark source archive path")
            if member.isdir():
                continue
            if not member.isfile() or path.parts[:2] != ("src", "xordination"):
                raise ValueError("Benchmark source must contain regular product files")
            destination = target.joinpath(*path.parts)
            destination.parent.mkdir(parents=True, exist_ok=True)
            stream = archive.extractfile(member)
            if stream is None:
                raise ValueError("Benchmark source file is unavailable")
            destination.write_bytes(stream.read())
    source = target / "src"
    if not (source / "xordination" / "__init__.py").is_file():
        raise ValueError("Baseline does not contain Xordination product source")
    return source


def run_measurement(source, output, config, env, session_id):
    command = [sys.executable, str(ROOT / "scripts" / "testing" / "benchmark.py"),
               "--source", str(source), "--output", str(output), "--warmup", "1",
               "--payload-bytes", "256", "--seed", "0", "--session-id", session_id]
    for name in DEFAULT_CONFIG:
        command.extend(["--" + name, str(config[name])])
    subprocess.run(command, cwd=ROOT, env=env, check=True, timeout=300)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--event", default=os.environ.get("GITHUB_EVENT_NAME", "workflow_dispatch"))
    parser.add_argument("--event-file", default=os.environ.get("GITHUB_EVENT_PATH"))
    args = parser.parse_args(argv)
    payload = json.loads(Path(args.event_file).read_text()) if args.event_file else {}
    config = benchmark_config(args.event, payload)
    revision = baseline_ref(args.event, payload)
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    env = dict(os.environ)
    # Coverage is a separate selected-suite measurement, not benchmark overhead.
    env.pop("COVERAGE_PROCESS_START", None)
    env.pop("COVERAGE_FILE", None)
    report = {"schema": 1, "config": config, "event": args.event,
              "base_commit": revision, "head_commit": os.environ.get("GITHUB_SHA"),
              "timing_gate": False, "comparison": "unavailable"}
    session_id = uuid.uuid4().hex
    report["session_id"] = session_id
    try:
        with tempfile.TemporaryDirectory(prefix="xordination-benchmark-base-") as folder:
            source = None
            if revision:
                try:
                    source = extract_source(ROOT, revision, Path(folder))
                except (OSError, ValueError, tarfile.TarError, subprocess.SubprocessError) as error:
                    report["baseline_unavailable_reason"] = type(error).__name__
            else:
                report["baseline_unavailable_reason"] = "No baseline commit supplied"
            # Both measurements are uninstrumented. A known bad or incompatible
            # historical base cannot prevent the required head correctness run.
            base_verified = False
            if source is not None:
                try:
                    run_measurement(source, output / "base.json", config, env, session_id)
                except (OSError, subprocess.SubprocessError) as error:
                    report["baseline_measurement"] = "failed"
                    report["baseline_failure"] = type(error).__name__
                    report["baseline_exit_code"] = getattr(error, "returncode", None)
                    print("Baseline benchmark failed; head correctness still runs and no timing comparison is authorized.",
                          file=sys.stderr)
                else:
                    base_verified = True
                    report["baseline_measurement"] = "passed"
            run_measurement(ROOT / "src", output / "head.json", config, env, session_id)
            if base_verified:
                subprocess.run([sys.executable, str(ROOT / "scripts" / "testing" / "benchmark.py"),
                                "--compare-base", str(output / "base.json"),
                                "--compare-head", str(output / "head.json"),
                                "--output", str(output / "comparison.json")],
                               cwd=ROOT, env=env, check=True, timeout=30)
                report["comparison"] = "reported"
    except (OSError, ValueError, subprocess.SubprocessError):
        report["measurement"] = "failed"
        raise
    else:
        report["measurement"] = "passed"
    finally:
        (output / "ci-context.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
