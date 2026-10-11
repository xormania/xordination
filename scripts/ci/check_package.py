"""Build the source archive, rebuild its wheel, and validate a clean installation."""

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import tarfile
import zipfile


def run(argv, *, cwd, env=None):
    subprocess.run(argv, cwd=cwd, env=env, check=True)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dist", default="dist")
    args = parser.parse_args(argv)
    repo = Path.cwd().resolve()
    target = Path(args.dist).resolve()
    target.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="xordination-package-") as folder:
        staging = Path(folder)
        archives = staging / "archives"
        wheels = staging / "wheels"
        archives.mkdir()
        wheels.mkdir()
        run([sys.executable, "-c",
             "import sys; from setuptools.build_meta import build_sdist; build_sdist(sys.argv[1])",
             str(archives)], cwd=repo)
        sources = list(archives.glob("*.tar.gz"))
        if len(sources) != 1:
            raise ValueError("Expected exactly one newly built source archive")
        with tarfile.open(sources[0]) as archive:
            archived = {name.split("/", 1)[1] for name in archive.getnames() if "/" in name}
            expected = {"LICENSE", "NOTICE", "README.md", "AGENTS.md", "Dockerfile", "pyproject.toml", "MANIFEST.in"}
            for folder, pattern in (("src", "*.py"), ("tests", "*.py"), ("docs", "*.md"),
                                    ("scripts/ci", "*.py"), ("scripts/testing", "*.py")):
                expected.update(str(path.relative_to(repo)) for path in (repo / folder).rglob(pattern))
            missing = expected - archived
            if missing:
                raise ValueError("Source archive omitted inputs: " + ", ".join(sorted(missing)))
        run([sys.executable, "-m", "pip", "wheel", "--no-deps", "--no-build-isolation",
             "--wheel-dir", str(wheels), str(sources[0])], cwd=staging)
        built = list(wheels.glob("*.whl"))
        if len(built) != 1:
            raise ValueError("Expected exactly one source-archive wheel")
        with zipfile.ZipFile(built[0]) as wheel:
            names = wheel.namelist()
            for license_name in ("LICENSE", "NOTICE"):
                matching = [name for name in names if name.endswith("/" + license_name)
                            and ".dist-info/" in name]
                if len(matching) != 1 or wheel.read(matching[0]) != (repo / license_name).read_bytes():
                    raise ValueError("Wheel did not preserve " + license_name)
        environment = staging / "installed"
        run([sys.executable, "-m", "venv", "--without-pip", str(environment)], cwd=staging)
        python = environment / ("Scripts/python.exe" if os.name == "nt" else "bin/python")
        # pip's target-interpreter option installs properly without re-seeding pip.
        run([sys.executable, "-m", "pip", "--python", str(python), "install", "--no-deps",
             "--no-index", str(built[0])], cwd=staging)
        clean = dict(os.environ)
        clean.pop("PYTHONPATH", None)
        clean.pop("PYTHONHOME", None)
        smoke = r'''
import importlib, importlib.metadata, json, pathlib, sys, tempfile
repo = pathlib.Path(sys.argv[1]).resolve()
package = importlib.import_module("xordination")
version = importlib.metadata.version("xordination")
assert package.__version__ == version
for name in ("xordination", "xordination.adapters", "xordination.cli", "xordination.client",
             "xordination.mcp", "xordination.rpc", "xordination.server", "xordination.store"):
    module = importlib.import_module(name)
    assert repo not in pathlib.Path(module.__file__).resolve().parents, name + " loaded checkout source"
from xordination.store import Store
with tempfile.TemporaryDirectory() as folder:
    store = Store(pathlib.Path(folder) / "broker.sqlite3")
    try:
        store.create_space("package")
        alice = store.authenticate(store.join("package", "alice")["token"])
        bob = store.authenticate(store.join("package", "bob")["token"])
        message = store.send(alice, "bob", "Installed artifact", "package-smoke")
        assert store.inbox(bob)["messages"][0]["id"] == message["id"]
        store.ack(bob, message["id"])
        assert store.status(alice, message["id"])["receipts"]
    finally:
        store.close()
print(json.dumps({"version": version, "installed": package.__file__, "durable_operation": "passed"}))
'''
        run([str(python), "-I", "-c", smoke, str(repo)], cwd=staging, env=clean)
        command = environment / ("Scripts/xordination.exe" if os.name == "nt" else "bin/xordination")
        version = subprocess.check_output([str(command), "--version"], cwd=staging, env=clean, text=True).strip()
        run([str(command), "--help"], cwd=staging, env=clean)
        import shutil
        for artifact in (sources[0], built[0]):
            shutil.copy2(artifact, target / artifact.name)
        print(json.dumps({"package_version": version, "artifacts": [sources[0].name, built[0].name],
                          "runtime": sys.version.split()[0]}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

