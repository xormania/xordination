"""Measure the complete unittest suite, including threads and CLI subprocesses.

This is a development tool. It does not impose a coverage percentage gate.
"""

from __future__ import annotations

import argparse
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time


ROOT = Path(__file__).resolve().parents[2]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path,
                        help="CI selection JSON; omit to measure the full suite")
    parser.add_argument("--output", type=Path, default=ROOT / "coverage-artifacts",
                        help="directory for combined data and text, JSON, and HTML reports")
    parser.add_argument("--tier", choices=("all", "fast", "slow"), default="all",
                        help="measure only this contract tier (see run_contracts.py)")
    parser.add_argument("--parallel", action="store_true", help="run the selected cases as parallel shards")
    parser.add_argument("--merge", type=Path, action="append", default=[],
                        help="combined data file from another tier to include in the reports")
    args = parser.parse_args()
    try:
        import coverage
    except ImportError:
        parser.exit(2, "Install development dependencies before measuring coverage: "
                    'python -m pip install ".[ci]"\n')

    from select_checks import FAST_GROUPS, GROUPS

    selection = (json.loads(args.selection.read_text(encoding="utf-8"))
                 if args.selection else {"groups": list(GROUPS), "full": True})
    groups = selection.get("groups", [])
    # The shared runner validates group names and rejects absent/empty suites.
    full = isinstance(groups, list) and set(groups) == set(GROUPS)
    measured_groups = [group for group in groups if args.tier == "all" or (group in FAST_GROUPS) == (args.tier == "fast")]
    cli_expected = (full and args.tier != "fast") or any(
        group in measured_groups for group in ("cli", "end_to_end", "behavior"))
    merged = [path.resolve() for path in args.merge]
    for path in merged:
        if not path.is_file():
            parser.exit(2, f"Coverage data to merge is missing: {path}\n")

    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=True)
    source = ROOT / "src" / "xordination"
    # Separate each invocation before combining so an earlier or concurrent run
    # cannot silently contribute its execution evidence to this baseline.
    with tempfile.TemporaryDirectory(prefix=".measurement-", dir=output) as temporary:
        working = Path(temporary)
        selected = working / "selection.json"
        selected.write_text(json.dumps(selection), encoding="utf-8")
        command = [sys.executable, str(ROOT / "scripts" / "ci" / "run_contracts.py"),
                   "--selection", str(selected), "--report", str(output / "test-execution.json"),
                   "--tier", args.tier] + (["--parallel"] if args.parallel else [])
        config = working / "coverage.ini"
        config.write_text(
            "[run]\n"
            "branch = true\n"
            "concurrency = thread\n"
            "parallel = true\n"
            "sigterm = true\n"
            f"data_file = {working / '.coverage'}\n"
            f"source = {source}\n"
            # Owned fake harnesses intentionally execute no product source.
            "disable_warnings = no-data-collected\n"
            "\n[report]\n"
            "precision = 2\n"
            "show_missing = true\n",
            encoding="utf-8",
        )
        env = dict(os.environ)
        env["COVERAGE_PROCESS_START"] = str(config)
        # sitecustomize starts public process_startup() before unittest imports
        # product modules. Absolute paths survive subprocess cwd changes. The
        # coverage path also supports children using another Python installation
        # with the same interpreter version and inherited environment.
        paths = [str(Path(__file__).resolve().parent), str(ROOT / "src"),
                 str(Path(coverage.__file__).resolve().parent.parent)]
        if env.get("PYTHONPATH"):
            paths.append(env["PYTHONPATH"])
        env["PYTHONPATH"] = os.pathsep.join(paths)
        started = time.monotonic()
        result = subprocess.run(command, cwd=ROOT, env=env, check=False)
        duration = time.monotonic() - started
        process_data = sorted(working.glob(".coverage.*"))

        measured_processes = 0
        cli_processes = 0
        unreadable = []
        for filename in list(process_data):
            data = coverage.CoverageData(basename=str(filename))
            try:
                data.read()
            except coverage.CoverageException:
                # Contracts SIGKILL stubborn fixture processes on purpose; one killed while
                # saving leaves partial data. It is reported, never silently combined.
                unreadable.append(filename.name)
                process_data.remove(filename)
                filename.rename(filename.with_name("unreadable" + filename.name))
                continue
            measured = [name for name in data.measured_files() if data.arcs(name)]
            measured_processes += bool(measured)
            cli_processes += any(Path(name) == source / "__main__.py" for name in measured)

        combined = coverage.Coverage(data_file=str(output / ".coverage"),
                                     config_file=str(config))
        # Data merged from another tier carries its own processes; it adds lines, never this run's evidence.
        combined.combine(data_paths=[str(working), *map(str, merged)], strict=True, keep=True)
        combined.save()
        branch_data_collected = combined.get_data().has_arcs()
        report = io.StringIO()
        combined.report(file=report)
        (output / "report.txt").write_text(report.getvalue(), encoding="utf-8")
        combined.json_report(outfile=str(output / "coverage.json"))
        combined.html_report(directory=str(output / "html"),
                             title="Xordination branch coverage baseline")

    totals = json.loads((output / "coverage.json").read_text(encoding="utf-8"))["totals"]
    statement_percent = (100 * totals["covered_lines"] / totals["num_statements"]
                         if totals["num_statements"] else 100.0)
    branch_percent = None
    if branch_data_collected:
        branch_percent = (100 * totals["covered_branches"] / totals["num_branches"]
                          if totals["num_branches"] else 100.0)
    metadata = {
        "python": sys.version,
        "coverage": coverage.__version__,
        "canonical_python": sys.version_info[:2] == (3, 12),
        "command": command,
        "groups": groups,
        "tier": args.tier,
        "merged": [str(path) for path in merged],
        "full": full,
        "selection": selection,
        "duration_seconds": round(duration, 3),
        "tests_exit_code": result.returncode,
        "execution_census": (json.loads((output / "test-execution.json").read_text(encoding="utf-8"))
                             if (output / "test-execution.json").is_file() else None),
        "process_data_files": len(process_data),
        "unreadable_process_data": unreadable,
        "product_processes": measured_processes,
        "cli_processes": cli_processes,
        "cli_coverage_expected": cli_expected,
        "statement_percent": statement_percent,
        "branch_percent": branch_percent,
        "branch_data_collected": branch_data_collected,
        "totals": totals,
    }
    (output / "baseline.json").write_text(json.dumps(metadata, indent=2) + "\n", encoding="utf-8")
    print(report.getvalue(), end="")
    print(f"Groups: {', '.join(groups)}; full suite: {full}")
    branch_label = f"{branch_percent:.2f}%" if branch_percent is not None else "not measured"
    print(f"Statements: {statement_percent:.2f}%; branches: {branch_label}")
    print(f"Combined {measured_processes} product processes, including {cli_processes} CLI processes.")
    if unreadable:
        print(f"Set aside {len(unreadable)} partial data file(s) from processes killed while saving.")
    print(f"Reports: {output}")
    if result.returncode:
        return result.returncode
    if cli_expected and (measured_processes < 2 or not cli_processes):
        print("Incomplete baseline: real CLI subprocess coverage was not collected.", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
