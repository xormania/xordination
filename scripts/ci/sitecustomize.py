"""Start coverage only inside the coverage helper's child environment."""

import os

if os.environ.get("COVERAGE_PROCESS_START"):
    import coverage

    coverage.process_startup()
