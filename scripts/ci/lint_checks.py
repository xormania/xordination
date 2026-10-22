"""Prove the pinned Ruff enforces every selected security rule with the repository's own configuration."""

from pathlib import Path
import json
import shutil
import subprocess
import sys
import tempfile
try:
    import tomllib
except ModuleNotFoundError:  # Python 3.10; the lint extra installs the backport.
    import tomli as tomllib

ROOT = Path(__file__).resolve().parents[2]
# Parsed by Ruff only; never imported or executed. Each rule has an unsafe and a safe form.
SAMPLES = {
    "S102": ('exec("value = 1")\n', 'import json\n\njson.loads("1")\n'),
    "S103": ('import os\n\nos.chmod("example", 0o777)\n', 'import os\n\nos.chmod("example", 0o600)\n'),
    "S104": ('import socket\n\nsocket.socket().bind(("0.0.0.0", 8080))\n',
             'import socket\n\nsocket.socket().bind(("127.0.0.1", 8080))\n'),
    "S202": ('import tarfile\n\nwith tarfile.open("example.tar") as archive:\n    archive.extractall()\n',
             'import tarfile\n\nwith tarfile.open("example.tar") as archive:\n    archive.extractall(filter="data")\n'),
    "S301": ('import pickle\n\npickle.loads(b"example")\n', 'import json\n\njson.loads("1")\n'),
    "S302": ('import marshal\n\nmarshal.loads(b"example")\n', 'import json\n\njson.loads("1")\n'),
    "S306": ('import tempfile\n\ntempfile.mktemp()\n', 'import tempfile\n\ntempfile.mkstemp()\n'),
    "S307": ('eval("1 + 1")\n', 'import ast\n\nast.literal_eval("1")\n'),
    "S323": ('import ssl\n\nssl._create_unverified_context()\n', 'import ssl\n\nssl.create_default_context()\n'),
    "S324": ('import hashlib\n\nhashlib.md5(b"example")\n', 'import hashlib\n\nhashlib.sha256(b"example")\n'),
    "S501": ('import requests\n\nrequests.get("https://example.invalid", verify=False, timeout=5)\n',
             'import requests\n\nrequests.get("https://example.invalid", verify=True, timeout=5)\n'),
    "S506": ('import yaml\n\nyaml.load("{}", Loader=yaml.Loader)\n', 'import yaml\n\nyaml.safe_load("{}")\n'),
    "S602": ('import subprocess\n\nsubprocess.run("echo example", shell=True, check=True)\n',
             'import subprocess\n\nsubprocess.run(["echo", "example"], check=True)\n'),
    "S604": ('import custom_process\n\ncustom_process.run("example", shell=True)\n',
             'import custom_process\n\ncustom_process.run(["example"], shell=False)\n'),
    "S605": ('import os\n\nos.system("echo example")\n', 'import subprocess\n\nsubprocess.run(["echo", "example"], check=True)\n'),
    "S608": ('def find(cursor, value):\n    cursor.execute("SELECT * FROM t WHERE id = \'%s\'" % value)\n',
             'def find(cursor, value):\n    cursor.execute("SELECT * FROM t WHERE id = ?", (value,))\n'),
    "S609": ('import subprocess\n\nsubprocess.Popen(["chmod", "777", "*.py"], shell=True)\n',
             'import subprocess\n\nsubprocess.Popen(["chmod", "600", "example.py"], shell=False)\n'),
}


def findings(root):
    result = subprocess.run([shutil.which("ruff") or "ruff", "check", "--no-cache", "--output-format", "json", "."],
                            cwd=root, capture_output=True, text=True, timeout=60)
    if result.returncode not in (0, 1):
        sys.exit("ruff failed: " + result.stderr)
    return {(item["code"], Path(item["filename"]).name) for item in json.loads(result.stdout)}


def main():
    selected = {rule for rule in tomllib.loads((ROOT / "pyproject.toml").read_text())["tool"]["ruff"]["lint"]["select"]
                if rule.startswith("S")}
    if selected != set(SAMPLES):
        sys.exit(f"Security rules and samples differ: unsampled {sorted(selected - set(SAMPLES))}, "
                 f"unselected {sorted(set(SAMPLES) - selected)}")
    with tempfile.TemporaryDirectory() as directory:
        root = Path(directory)
        shutil.copyfile(ROOT / "pyproject.toml", root / "pyproject.toml")
        for code, (unsafe, _) in SAMPLES.items():
            (root / (code.lower() + ".py")).write_text(unsafe)
        reported = findings(root)
        missing = {(code, code.lower() + ".py") for code in SAMPLES} - reported
        if missing:
            sys.exit(f"Selected security rules not enforced: {sorted(missing)}")
        for code, (_, safe) in SAMPLES.items():
            (root / (code.lower() + ".py")).write_text(safe)
        if reported := findings(root):
            sys.exit(f"Safe samples reported: {sorted(reported)}")
    print(f"{len(SAMPLES)} selected security rules enforced; their safe forms pass")


if __name__ == "__main__":
    main()
