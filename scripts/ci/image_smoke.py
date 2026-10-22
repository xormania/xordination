"""Exercise the nonroot broker image against durable state; remove owned resources.

A host client installed from the package artifact and a client inside the
broker container must reach the same broker and state (cross-profile AC-14).
"""
import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
from urllib.error import HTTPError, URLError
from urllib.request import ProxyHandler, Request, build_opener
import uuid


def docker(*args):
    result = subprocess.run(["docker", *args], check=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, text=True, timeout=60)
    return result.stdout.strip()


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--image", required=True)
    parser.add_argument("--dist", default="dist", help="Directory holding exactly one built wheel")
    args = parser.parse_args(argv)
    wheels = list(Path(args.dist).glob("xordination-*.whl"))
    if len(wheels) != 1:
        raise ValueError("Cross-profile check needs exactly one package wheel in " + args.dist)
    with tempfile.TemporaryDirectory(prefix="xordination-client-") as folder:
        return smoke(args.image, install_client(wheels[0], Path(folder)))


def install_client(wheel, folder):
    """Install the package artifact into a fresh environment outside the checkout."""
    environment = folder / "venv"
    subprocess.run([sys.executable, "-m", "venv", "--without-pip", str(environment)], check=True, timeout=120)
    python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
    subprocess.run([sys.executable, "-m", "pip", "--python", str(python), "install", "--no-deps",
                    "--no-index", str(wheel)], check=True, timeout=120, stdout=subprocess.DEVNULL)
    env = {key: value for key, value in os.environ.items() if key not in {"PYTHONPATH", "PYTHONHOME"}}
    env["PATH"] = str(python.parent) + os.pathsep + env.get("PATH", "")
    executable = python.parent / ("xordination.exe" if os.name == "nt" else "xordination")
    def client(*command, token):
        result = subprocess.run([str(executable), *command], cwd=folder, env={**env, "XORDINATION_TOKEN": token},
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=60)
        if result.returncode:
            raise ValueError("Installed host client failed: " + result.stderr[-2000:])
        return json.loads(result.stdout)
    return client


def smoke(image, host_client):
    prefix = "xordination-smoke-" + uuid.uuid4().hex
    volume, container = prefix + "-state", prefix + "-broker"
    volume_created = container_created = False
    one_off_names = []
    opener = build_opener(ProxyHandler({}))
    def one_off(*command):
        name = prefix + "-once-" + str(len(one_off_names))
        one_off_names.append(name)
        return docker("run", "--rm", "--name", name, *command)
    try:
        volume_created = True
        docker("volume", "create", volume)
        uid = one_off("--entrypoint", "python", image,
                      "-c", "import os; print(os.getuid())")
        if uid != "10001":
            raise ValueError("Broker image must execute as UID 10001")
        def bootstrap(*command):
            return json.loads(one_off("--volume", volume + ":/state", image, *command))
        state = "/state/broker.sqlite3"
        bootstrap("space", "--state", state, "image")
        alice = bootstrap("join", "--state", state, "--space", "image", "--participant", "alice")["token"]
        bob = bootstrap("join", "--state", state, "--space", "image", "--participant", "bob")["token"]
        def start():
            nonlocal container_created
            container_created = True
            docker("run", "--detach", "--name", container, "--publish", "127.0.0.1::8765",
                   "--volume", volume + ":/state", image)
            mapping = docker("port", container, "8765/tcp")
            if not mapping.startswith("127.0.0.1:") or not mapping[10:].isdigit():
                raise ValueError("Unexpected loopback-only broker port mapping")
            endpoint = "http://" + mapping
            deadline = time.monotonic() + 10
            while True:
                try:
                    with opener.open(endpoint + "/health", timeout=1) as response:
                        value = json.load(response)
                    if value.get("ok") is True and value.get("service") == "xordination":
                        return endpoint
                except (OSError, URLError, ValueError):
                    pass
                if time.monotonic() >= deadline:
                    raise ValueError("Broker image never became healthy")
                time.sleep(.1)
        def call(endpoint, token, operation, **params):
            request = Request(endpoint + "/rpc", data=json.dumps({"operation": operation, "params": params}).encode(),
                              headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
            with opener.open(request, timeout=5) as response:
                value = json.load(response)
            if value.get("ok") is not True:
                raise ValueError("Broker operation failed: " + operation)
            return value["result"]
        endpoint = start()
        try:
            call(endpoint, "invalid-smoke-token", "peers")
        except HTTPError as error:
            rejection = json.load(error)
            if error.code != 400 or rejection.get("error", {}).get("code") != "unauthorized":
                raise ValueError("Unexpected invalid-token response") from None
        else:
            raise ValueError("Broker accepted an invalid participant token")
        first = call(endpoint, alice, "send", to="bob", body="Durable image smoke", key="image-message")
        repeated = call(endpoint, alice, "send", to="bob", body="Durable image smoke", key="image-message")
        if repeated["id"] != first["id"]:
            raise ValueError("Image broker lost send idempotency")
        received = call(endpoint, bob, "inbox")["messages"]
        if [message["id"] for message in received] != [first["id"]]:
            raise ValueError("Image broker did not deliver the accepted message")
        call(endpoint, bob, "ack", message_id=first["id"])
        reply = call(endpoint, bob, "send", to="alice", body="Received", key="image-reply", reply_to=first["id"])
        # Cross-profile: the installed host package and the container's own client share one broker.
        readiness = host_client("readiness", "--endpoint", endpoint, token=alice)["levels"]
        if (readiness["package"]["checks"].get("installed_identity") != "passed"
                or readiness["service"]["status"] != "passed"
                or readiness["service"]["participant"]["participant"] != "alice"
                or readiness["service"]["broker_version"] != readiness["package"]["version"]
                or readiness["communication"]["status"] != "unverified"):
            raise ValueError("Installed host client readiness against the image is wrong: " + json.dumps(readiness))
        crossed = host_client("call", "send", "--endpoint", endpoint, "--params", json.dumps(
            {"to": "bob", "body": "Host package to container", "key": "cross-profile"}), token=alice)
        def container_client(token, *command):
            result = subprocess.run(["docker", "exec", "--env", "XORDINATION_TOKEN", container, "xordination",
                                     *command, "--endpoint", "http://127.0.0.1:8765"],
                                    env={**os.environ, "XORDINATION_TOKEN": token}, check=True,
                                    stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, timeout=60)
            return json.loads(result.stdout)
        if crossed["id"] not in [message["id"] for message in container_client(bob, "call", "inbox")["messages"]]:
            raise ValueError("Container-local client did not see the host package client's message")
        container_client(bob, "call", "ack", "--params", json.dumps({"message_id": crossed["id"]}))
        if not host_client("call", "status", "--endpoint", endpoint, "--params",
                           json.dumps({"message_id": crossed["id"]}), token=alice)["acknowledged"]:
            raise ValueError("Host package client did not see the container-local receipt")
        docker("stop", "--time", "2", container)
        docker("rm", container)
        container_created = False
        endpoint = start()
        retained = call(endpoint, alice, "status", message_id=first["id"])
        if not retained["receipts"] or [message["id"] for message in retained["replies"]] != [reply["id"]]:
            raise ValueError("Image restart lost durable receipts or replies")
        if call(endpoint, alice, "inbox")["messages"][0]["id"] != reply["id"]:
            raise ValueError("Image restart lost the recipient inbox")
        if not host_client("call", "status", "--endpoint", endpoint, "--params",
                           json.dumps({"message_id": crossed["id"]}), token=alice)["receipts"]:
            raise ValueError("Image restart lost the cross-profile receipt")
        print(json.dumps({"uid": int(uid), "authentication": "passed", "idempotency": "passed",
                          "durable_reopen": "passed", "cross_profile": "passed", "resources": "ephemeral"}))
    finally:
        # The UUID names are exclusively owned by this invocation; no global prune.
        failed = sys.exc_info()[0] is not None
        cleanup_errors = []
        def clean(kind, *command):
            try:
                result = subprocess.run(["docker", *command], stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, timeout=60)
            except (OSError, subprocess.SubprocessError):
                cleanup_errors.append(kind)
                return
            if result.returncode and not (kind == "container" and b"No such container" in result.stderr):
                cleanup_errors.append(kind)
        for name in one_off_names + ([container] if container_created else []):
            clean("container", "rm", "--force", name)
        if volume_created:
            clean("volume", "volume", "rm", volume)
        if cleanup_errors and not failed:
            raise ValueError("Image smoke cleanup failed: " + ", ".join(cleanup_errors))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
