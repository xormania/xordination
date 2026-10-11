# Coverage baseline

The canonical baseline uses Python 3.12 and development dependency `coverage==7.16.2`. Runtime dependencies remain empty. Run from a repository checkout:

```sh
python -m pip install ".[ci]"
python scripts/ci/coverage_baseline.py --output coverage-artifacts
```

CI supplies `--selection selection.json` to run the same selected contract groups used on the other supported Python versions. The helper invokes the shared `run_contracts.py` runner once. Test failures remain failures; there is no coverage percentage threshold.

The output directory contains:

| Artifact | Purpose |
| --- | --- |
| `.coverage` | Combined execution data from this invocation only |
| `report.txt` | Module summary with missing lines and branches |
| `coverage.json` | Exact statement and branch counts, missing lines, and missing branch destinations |
| `html/` | Source-level inspection |
| `baseline.json` | Python and coverage versions, selected groups, full-suite flag, duration, test exit code, and collected product/CLI process counts |

Statement coverage and branch coverage are reported separately. Coverage.py's ordinary `Cover` column combines statement and branch opportunities; it is not the branch percentage. Partial selections retain the complete product source denominator and identify their selected groups and `full: false` in `baseline.json`. A selection that executes no product code reports `branch_percent: null` and `branch_data_collected: false`, rather than claiming branch coverage. Compare baseline totals only for matching scope, Python version, and coverage version. A partial report is evidence of the selected suite, not a whole-suite regression.

## Measurement boundaries

The generated configuration enables branch measurement, `concurrency = thread`, parallel data files, and SIGTERM saving. The helper adds its `sitecustomize.py`, the absolute source directory, and coverage's import directory to the child environment's `PYTHONPATH`. The startup hook calls public `coverage.process_startup()` when `COVERAGE_PROCESS_START` is set. This starts measurement before unittest imports product code and follows inherited environments even when CLI children change their working directory. No installed Python configuration is edited.

Only `src/xordination` is measured. Test code, CI helpers, the standard library, and fake native harness programs executed with `python -c` do not enter the product denominator. A fresh temporary directory prevents older or simultaneous data files from contributing to the report. Process data is combined with the supported `Coverage.combine()` API before any reports are generated.

For selections containing `cli` or `end_to_end`, the helper requires independently saved subprocess data with executed `xordination/__main__.py` arcs. This detects loss of actual CLI subprocess instrumentation rather than accepting a parent-only report. Other selections report whether CLI coverage was expected and do not claim subprocess evidence they did not collect.

A subprocess must inherit the startup environment and use ordinary Python startup. `-S`, isolated environment settings, incompatible interpreter versions, or replacement of `PYTHONPATH` can prevent instrumentation. SIGKILL, crashes that bypass cleanup, and forcibly terminated processes can lose unsaved coverage. Collected process counts prove saved evidence, not complete execution evidence from every abrupt termination. This baseline does not certify native vendor compatibility or model behavior.

## Measured full-suite baseline

The complete local baseline used Python 3.12.14 and Coverage.py 7.16.2. All 75 tests passed through the shared full-suite runner: 68 product contract tests and seven CI-selection tests. The unittest run took 6.064 seconds; total child-process duration was 6.556 seconds. Four product processes contributed data, including three actual CLI processes. CI-selection test source remains excluded from the product denominator.

| Module | Statement coverage | Branch coverage |
| --- | ---: | ---: |
| `adapters.py` | 60.87% | 43.56% |
| `cli.py` | 78.01% | 60.53% |
| `client.py` | 81.63% | 50.00% |
| `mcp.py` | 89.41% | 77.36% |
| `rpc.py` | 83.02% | 78.12% |
| `server.py` | 81.88% | 64.71% |
| `store.py` | 89.55% | 73.38% |
| **Total** | **1,361 / 1,747 = 77.90%** | **357 / 574 = 62.20%** |

`__init__.py` and `__main__.py` were fully measured and have no branch opportunities. The combined statement-and-branch display was 74.02%.

The largest measured gap is adapter launch/orchestration, especially `run_claude`, terminal output handling, capability/schema inspection, and adapter run loops. Further missing branches occur in CLI command dispatch, client endpoint validation and transport errors, broker request/error handling, and store validation/rollback cases. Use `coverage.json` or HTML to select a concrete behavior risk before adding a test. Exercised lines alone do not establish adequate assertions, and missing branches do not automatically justify another test.

## Primary references

- [Coverage.py 7.16.2 process startup API](https://coverage.readthedocs.io/en/7.16.2/api_module.html)
- [Process startup and shutdown requirements](https://coverage.readthedocs.io/en/7.16.2/subprocess.html)
- [Branch, thread, parallel, source, and SIGTERM settings](https://coverage.readthedocs.io/en/7.16.2/config.html)
- [Combining and reporting APIs](https://coverage.readthedocs.io/en/7.16.2/api_coverage.html)
- [Python 3.12 sitecustomize startup](https://docs.python.org/3.12/library/site.html)

Author attribution: xormania.
