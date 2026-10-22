"""Install a pinned dependency set named in pyproject.toml: an extra, or the build backend."""

import argparse
from pathlib import Path
import subprocess
import sys
import tomllib

ROOT = Path(__file__).resolve().parents[2]


def requirements(names):
    config = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    extras = config["project"]["optional-dependencies"]
    selected = []
    for name in names:
        selected += config["build-system"]["requires"] if name == "build" else extras[name]
    return selected


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("sets", nargs="+", help="optional-dependency extras, or 'build' for the build backend")
    args = parser.parse_args(argv)
    return subprocess.run([sys.executable, "-m", "pip", "install", *requirements(args.sets)], check=False).returncode


if __name__ == "__main__":
    raise SystemExit(main())
