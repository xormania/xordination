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
        result["entry_point"] = shutil.which("xordination")
        checks["console_entry_point"] = "passed" if declared and result["entry_point"] else "failed"
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
    return {"path": str(path.resolve()), "sha256": hashlib.sha256(data).hexdigest(),
            "run_id": report["run_id"], "source_sha256": report.get("source_sha256"),
            "status": report["status"], "pairs": pairs}


def communication_level(reports):
    if not reports:
        return {"status": "unverified", "reports": [],
                "detail": "No verified live evidence supplied; readiness never starts native sessions"}
    return {"status": "passed" if all(report["status"] == "pass" for report in reports) else "failed",
            "reports": reports,
            "basis": "Supplied operator_native verification reports, structurally checked, not re-verified"}


def adapter_level(harnesses, reports, binary=None):
    if not harnesses:
        return {"status": "unverified", "harnesses": {},
                "detail": "No harness selected; installed capabilities were not probed"}
    from .adapters import doctor
    # A passing exchange required observed native submissions to both participants' sessions.
    observed = {pair[side + "_harness"] for report in reports for pair in report["pairs"]
                if pair["status"] == "pass" for side in ("sender", "recipient")}
    results = {}
    for name in harnesses:
        capability = doctor(name, binary=binary)
        checks = {"installed_capability": {"supported": "passed", "unverified": "unverified"}
                  .get(capability["status"], "failed"),
                  "session_identity": "passed" if name in observed else "unverified"}
        results[name] = {"status": summarize(checks), "checks": checks, "doctor": capability}
    return {"status": summarize({name: value["status"] for name, value in results.items()}),
            "harnesses": results}


def report(*, state=None, endpoint=None, token=None, harnesses=(), binary=None, evidence=()):
    reports = [load_evidence(path) for path in evidence]
    levels = {"package": package_level(state), "service": service_level(endpoint, token),
              "adapter": adapter_level(list(dict.fromkeys(harnesses)), reports, binary),
              "communication": communication_level(reports)}
    return {"levels": levels, "passed": [name for name, level in levels.items() if level["status"] == "passed"],
            "native_sessions_started": False,
            "boundary": "Each level is reported separately. Communication readiness does not establish "
                        "application-work completion or useful model reasoning."}
