"""Separate package, service, adapter and communication readiness observations.

Nothing here launches a native session or calls a model. Communication
readiness comes only from a supplied operator-native verification report.
"""
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import shutil
import sys

from . import __version__
from .client import BrokerError, Client, health

MAX_EVIDENCE_BYTES = 4 * 1024 * 1024
HARNESSES = ("codex", "claude", "grok")


def summarize(checks):
    """A level passes only when every check passed; any failure is a failure."""
    values = set(checks.values())
    if "failed" in values:
        return "failed"
    return "passed" if values == {"passed"} else "unverified"


def _installed_by(distribution, command):
    """Whether the PATH command is a script recorded in this distribution's installed files.

    Another environment's or an unrelated `xordination` earlier on PATH would run
    different code; None means the distribution keeps no file record to check.
    """
    files = distribution.files
    if files is None:
        return None
    for item in files:
        if item.name.lower() in ("xordination", "xordination.exe"):
            try:
                if os.path.samefile(distribution.locate_file(item), command):
                    return True
            except OSError:
                continue
    return False


def package_level(state=None):
    module = Path(__file__).resolve().with_name("__init__.py")
    result = {"version": __version__, "module": str(module.parent),
              "python": platform.python_version(), "platform": sys.platform}
    checks = {}
    try:
        distribution = importlib.metadata.distribution("xordination")
    except importlib.metadata.PackageNotFoundError:
        distribution = None
    # A checkout on sys.path beside an unrelated installation is not that installation.
    located = distribution and Path(distribution.locate_file("xordination/__init__.py")).resolve()
    if distribution is None or located != module:
        checks["installed_identity"] = "failed"
        result["detail"] = "Imported module does not belong to an installed xordination distribution"
    else:
        result["installed_version"] = distribution.version
        checks["installed_identity"] = "passed" if distribution.version == __version__ else "failed"
        declared = any(point.group == "console_scripts" and point.name == "xordination"
                       and point.value == "xordination.cli:main" for point in distribution.entry_points)
        command = result["entry_point"] = shutil.which("xordination")
        owned = command and _installed_by(distribution, command)
        if not declared or not command or owned is False:
            checks["console_entry_point"] = "failed"
            if command and owned is False:
                result["detail"] = "The xordination command on PATH is not this distribution's console script"
        elif owned is None:
            checks["console_entry_point"] = "unverified"
            result["detail"] = "The distribution records no installed files; the PATH command is unattributed"
        else:
            checks["console_entry_point"] = "passed"
    if state is None:
        checks["state_location"] = "unverified"
    else:
        # Observe without creating or opening the store; a running broker may own it.
        path = Path(state).absolute()
        result["state"] = str(path)
        usable = path.parent.is_dir() and os.access(path.parent, os.W_OK | os.X_OK)
        if path.exists():
            usable = usable and path.is_file() and os.access(path, os.R_OK | os.W_OK)
        checks["state_location"] = "passed" if usable else "failed"
    result["checks"] = checks
    result["status"] = summarize(checks)
    return result


def service_level(endpoint=None, token=None):
    if not endpoint:
        return {"status": "unverified", "checks": {"health": "unverified"},
                "detail": "No broker endpoint supplied"}
    result = {"endpoint": endpoint}
    checks = result["checks"] = {}
    try:
        observed = health(endpoint)
    except BrokerError as error:
        checks["health"] = "failed"
        result["error"] = error.code
    else:
        checks["health"] = "passed"
        result["broker_version"] = observed.get("version")
        if token is None:
            checks["authenticated_operation"] = "unverified"
            result["detail"] = "Health alone; supply a participant token for an authenticated operation"
        else:
            try:
                identity = Client(endpoint, token, timeout=10).call("identity")
            except BrokerError as error:
                checks["authenticated_operation"] = "failed"
                result["error"] = error.code
            else:
                checks["authenticated_operation"] = "passed"
                result["participant"] = {key: identity.get(key) for key in
                                         ("space", "participant", "generation", "harness", "active")}
    result["status"] = summarize(checks)
    return result


def _digest(value):
    """The live verifier's canonical JSON digest."""
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
                                     allow_nan=False).encode()).hexdigest()


RUNNER = "scripts/testing/live.py"
_HEX64 = re.compile(r"[0-9a-f]{64}")


def source_sha256(package, runner_sha256):
    """`live.py`'s source fingerprint: its own bytes' digest plus every module of the package.

    The verifier records this for its checkout's src/xordination; readiness recomputes it
    from the imported package, taking the runner digest from the report.
    """
    files = {RUNNER: runner_sha256}
    for path in sorted(Path(package).glob("*.py")):
        files["src/xordination/" + path.name] = hashlib.sha256(path.read_bytes()).hexdigest()
    return _digest(files)


def _invalid(path):
    return ValueError("Evidence " + str(path) + " is not a complete operator_native verification report")


def load_evidence(path):
    """Structurally validate a retained `live.py verify` report; it is not re-verified here."""
    path = Path(path)
    if path.stat().st_size > MAX_EVIDENCE_BYTES:
        raise _invalid(path)
    data = path.read_bytes()
    try:
        report = json.loads(data)
    except ValueError:
        raise _invalid(path) from None
    if (not isinstance(report, dict) or report.get("schema_version") != 1
            or report.get("evidence_mode") != "operator_native" or report.get("status") not in {"pass", "fail"}
            or not isinstance(report.get("run_id"), str) or not isinstance(report.get("cases"), list)
            or not isinstance(report.get("errors"), list)
            or not isinstance(report.get("configured_participants"), list)):
        raise _invalid(path)
    harness = {}
    for participant in report["configured_participants"]:
        if (not isinstance(participant, dict) or not isinstance(participant.get("name"), str)
                or participant.get("harness") not in HARNESSES):
            raise _invalid(path)
        harness[participant["name"]] = participant["harness"]
    pairs = []
    for case in report["cases"]:
        if (not isinstance(case, dict) or case.get("status") not in {"pass", "fail"}
                or case.get("sender") not in harness or case.get("recipient") not in harness
                or not isinstance(case.get("scenario"), str)):
            raise _invalid(path)
        pairs.append({"sender": case["sender"], "sender_harness": harness[case["sender"]],
                      "recipient": case["recipient"], "recipient_harness": harness[case["recipient"]],
                      "scenario": case["scenario"], "status": case["status"]})
    passed = sum(pair["status"] == "pass" for pair in pairs)
    # The verifier's own pass rule; an inconsistent summary is rejected, not trusted.
    consistent = (report.get("required_cases") == len(pairs) and report.get("passed_cases") == passed
                  and (report["status"] == "pass") == bool(pairs and passed == len(pairs) and not report["errors"]))
    if not consistent:
        raise _invalid(path)
    # The verifier hashed its validated configuration; its cases enumerate scenarios x pairs in order.
    scenarios = list(dict.fromkeys(pair["scenario"] for pair in pairs))
    config = {"participants": report["configured_participants"], "scenarios": scenarios,
              "pairs": [[pair["sender"], pair["recipient"]] for pair in pairs
                        if scenarios and pair["scenario"] == scenarios[0]]}
    used = {pair[side] for pair in pairs for side in ("sender", "recipient")}
    return {"path": str(path.resolve()), "sha256": hashlib.sha256(data).hexdigest(),
            "run_id": report["run_id"], "source_sha256": report.get("source_sha256"),
            "runner_sha256": report.get("runner_sha256"), "config_sha256": report.get("config_sha256"),
            "config_consistent": report.get("config_sha256") == _digest(config),
            "participants": [{key: participant.get(key) for key in ("name", "harness", "version", "binary")}
                             for participant in report["configured_participants"] if participant["name"] in used],
            "status": report["status"], "pairs": pairs}


def evidence_scope(evidence, probes, package=None):
    """Whether a report describes this runtime; anything unmatched or uncheckable is listed.

    A report certifies nothing unless its source fingerprint equals the imported package's,
    its configuration digest is consistent, and each exercised participant's recorded
    executable and version equal the current `doctor` observation for its harness.
    """
    package = package or Path(__file__).resolve().parent
    gaps = []
    runner, recorded = evidence.get("runner_sha256"), evidence.get("source_sha256")
    if not (isinstance(runner, str) and _HEX64.fullmatch(runner) and isinstance(recorded, str)):
        gaps.append("source_fingerprint_unverifiable")
    elif source_sha256(package, runner) != recorded:
        gaps.append("source_mismatch")
    if not evidence.get("config_consistent"):
        gaps.append("config_mismatch")
    for participant in evidence["participants"]:
        name, probe = participant["name"], probes.get(participant["harness"])
        if probe is None:
            gaps.append(name + ": harness_not_probed")
            continue
        binary, observed = participant.get("binary"), probe.get("binary")
        if not isinstance(binary, str) or not binary:
            gaps.append(name + ": binary_unrecorded")
        elif not isinstance(observed, str) or os.path.realpath(binary) != os.path.realpath(observed):
            gaps.append(name + ": binary_mismatch")
        version, current = participant.get("version"), probe.get("version") or {}
        if not isinstance(version, str) or not version:
            gaps.append(name + ": version_unrecorded")
        elif current.get("returncode") != 0 or current.get("value") != version:
            gaps.append(name + ": version_mismatch")
    return {"status": "unverified" if gaps else "current", "gaps": gaps}


def communication_level(reports):
    if not reports:
        return {"status": "unverified", "reports": [],
                "detail": "No verified live evidence supplied; readiness never starts native sessions"}
    current = [report for report in reports if report["scope"]["status"] == "current"]
    if any(report["status"] == "fail" for report in current):
        status = "failed"
    elif len(current) == len(reports):
        status = "passed"
    else:
        status = "unverified"
    result = {"status": status, "reports": reports,
              "basis": "Supplied operator_native verification reports, structurally checked, not re-verified; "
                       "only reports fingerprinted to this package and these harness executables count"}
    if status == "unverified":
        result["detail"] = "Evidence does not match, or cannot be matched to, the current runtime"
    return result


def adapter_level(harnesses, reports, probes):
    if not harnesses:
        return {"status": "unverified", "harnesses": {},
                "detail": "No harness selected; installed capabilities were not probed"}
    # A passing exchange required observed native submissions to both participants' sessions.
    observed = {pair[side + "_harness"] for report in reports if report["scope"]["status"] == "current"
                for pair in report["pairs"] if pair["status"] == "pass" for side in ("sender", "recipient")}
    results = {}
    for name in harnesses:
        capability = probes[name]
        checks = {"installed_capability": {"supported": "passed", "unverified": "unverified"}
                  .get(capability["status"], "failed"),
                  "session_identity": "passed" if name in observed else "unverified"}
        results[name] = {"status": summarize(checks), "checks": checks, "doctor": capability}
    return {"status": summarize({name: value["status"] for name, value in results.items()}),
            "harnesses": results}


def report(*, state=None, endpoint=None, token=None, harnesses=(), binary=None, evidence=()):
    reports = [load_evidence(path) for path in evidence]
    harnesses = list(dict.fromkeys(harnesses))
    probes = {}
    if harnesses:
        from .adapters import doctor
        probes = {name: doctor(name, binary=binary) for name in harnesses}
    for item in reports:
        # Only selected, probed harnesses can be matched; nothing native is probed implicitly.
        item["scope"] = evidence_scope(item, probes)
    levels = {"package": package_level(state), "service": service_level(endpoint, token),
              "adapter": adapter_level(harnesses, reports, probes),
              "communication": communication_level(reports)}
    return {"levels": levels, "passed": [name for name, level in levels.items() if level["status"] == "passed"],
            "native_sessions_started": False,
            "boundary": "Each level is reported separately. Communication readiness does not establish "
                        "application-work completion or useful model reasoning."}
