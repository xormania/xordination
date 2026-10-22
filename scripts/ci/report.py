"""Append the selection and any JSON evidence that exists to the job summary."""

import argparse
import json
import os
from pathlib import Path


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--selection", type=Path)
    parser.add_argument("evidence", nargs="*", help="TITLE=PATH pairs; absent files are omitted")
    args = parser.parse_args(argv)
    sections = []
    if args.selection:
        if args.selection.is_file():
            sections.append(("Selected checks", json.loads(args.selection.read_text(encoding="utf-8"))))
        else:
            sections.append((None, "Selection failed before publishing a result; this job cannot authorize a skip."))
    for item in args.evidence:
        title, _, path = item.partition("=")
        if Path(path).is_file():
            sections.append((title, json.loads(Path(path).read_text(encoding="utf-8"))))
    text = "".join(f"{body}\n\n" if title is None else f"{title}:\n\n```json\n{json.dumps(body, indent=2)}\n```\n\n"
                   for title, body in sections)
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with open(summary, "a", encoding="utf-8") as stream:
            stream.write(text)
    else:
        print(text, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
