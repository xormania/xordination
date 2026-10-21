# CI checks

CI starts on every pull request to `master`, push to `master`, and manual dispatch. It selects checks before Python setup or dependency installation. It does not use workflow path filters: a skipped workflow can leave a required check pending. CodeQL continues to perform its actual Python scan independently on every existing trigger.

The selector compares the complete event base with the tested commit. Pull requests use the checked-out merge tree and require that the advertised base is its ancestor; pushes use `before`. Both rename paths and deletions are retained using NUL-delimited Git records. Missing commits, malformed event/diff data, unknown paths, Git failures, and manual dispatch select everything. Selection publication failure makes the job fail. Mixed changes union their dependencies.

| Changed input | Contract groups | Package | Image | Benchmark |
| --- | --- | --- | --- | --- |
| `store.py` | store, adapters, server, cli, end_to_end, behavior, performance, live_acceptance, live_native_busy, live_tmux | yes | yes | yes |
| `adapters.py` | adapters, cli, behavior, live_acceptance, live_native_busy, live_tmux | yes | yes | no |
| `rpc.py` | adapters, behavior, live_acceptance, live_native_busy, live_tmux | yes | yes | no |
| `mcp.py` | adapters, mcp, cli, end_to_end, behavior, live_acceptance, live_native_busy, live_tmux | yes | yes | no |
| `server.py` | server, cli, end_to_end, behavior, performance, live_acceptance, live_native_busy, live_tmux | yes | yes | yes |
| `client.py` | adapters, mcp, server, cli, end_to_end, behavior, performance, live_acceptance, live_native_busy, live_tmux | yes | yes | yes |
| `cli.py` | adapters, mcp, server, cli, end_to_end, behavior, live_acceptance, live_native_busy, live_tmux | yes | yes | no |
| `__init__.py`, `__main__.py`, unknown runtime input | all | yes | yes | yes |
| Existing registered test module | its group | yes | no | when performance |
| `scripts/testing/benchmark.py` | performance | yes | no | yes |
| `scripts/testing/live.py` | live_acceptance, live_native_busy, live_tmux | yes | no | no |
| `scripts/testing/live_tmux.py` | live_tmux | yes | no | no |
| `tests/live_native_fixture.py` | live_native_busy | yes | no | no |
| New/shared test input, CI helper, workflow, unknown path | all | yes | yes | yes |
| `pyproject.toml` | all | yes | yes | yes |
| `MANIFEST.in` | all | yes | no | yes |
| `README.md`, `LICENSE`, `NOTICE`, `Dockerfile` | none | yes | yes | no |
| `docs/*.md`, `AGENTS.md` | none | yes | no | no |
| `.dockerignore` | none | no | yes | no |

The map includes lazy imports from MCP into adapters, public CLI dispatch, the real stdio subprocess in end-to-end/behavior tests, native RPC fixtures used by behavior, and broker inputs consumed by the performance driver. The live_acceptance group runs deterministic evidence-validator, held-request and passive-observer tests; live_native_busy exercises actual adapter/observer/broker orchestration with owned scripted native executables. Its passing control starts an idle recipient, primes a real unfinished native request, submits the correlated pair only after readiness, then verifies receipt/reply handling after release. Its planted no-readiness red requires the exact failure stage. The dedicated immutable native fixture selects only that group. live_tmux drives the managed launcher's supervisor through a subprocess stand-in for tmux, a real broker started through the launcher's broker window (the same `xordination.cli.serve` path as `xordination serve`) and scripted native participants; it requires no tmux installation. Real tmux command parsing is checked by the launcher's `self-check` on a private socket, outside CI. Selection never launches a paid native session. A full selection discovers every `test*.py`, including new modules. An absent or empty registered selected module is an error. Update the map when dependencies change; uncertainty should retain the full fallback.

Every selected case is required. The runner reports collected cases, starts, actual execution, skipped cases and unexercised cases separately. Required skips, fixture failures that prevent case execution, and `expectedFailure` markers fail the check. Intentional fault probes must assert the intended failure reason within an otherwise passing test. The canonical coverage artifact retains `test-execution.json`; other matrix jobs retain their census in the job summary.

The same selected groups run on Python 3.10, 3.12, and 3.14. The existing three-job graph is retained; documentation-only changes skip Python setup entirely in the 3.10/3.14 jobs. Those jobs validate source imports through the public CLI when contracts run. They no longer rebuild/install the same wheel. Python 3.12 builds one source archive, checks its included runtime/docs/tests/CI inputs, rebuilds a wheel from that archive, checks license notices, and exercises a fresh installation outside the checkout. The installed smoke explicitly rejects checkout imports and checks send/inbox/ack/status behavior. This trades repeated installed-artifact evidence on the other runtimes for the canonical installation plus their source contracts and CLI smoke.

Python 3.12 runs the selected suite once under branch/thread/subprocess coverage and uploads the report. Each report names the selected groups and whether it is full; partial reports are not whole-suite regressions. Manual dispatch refreshes the full baseline. See [coverage baseline](coverage-baseline.md) for measured gaps and process limitations. Coverage is a development-only `ci` extra; runtime dependencies stay empty. There is no percentage threshold. Source-archive checks retain the shared fixtures and both CI/testing drivers so a packaged source checkout can reproduce the tests.

Selected broker/benchmark inputs also run a small, uninstrumented performance measurement on Python 3.12. `benchmark_compare.py` extracts only the event base's regular product-source files and runs the current benchmark and shared helper against both base and head on the same runner. It does not execute the base's test/CI scripts. Both revisions use one shared measurement-session identifier, 8 work units per peer, 4 concurrent peers, 3 measured trials and 1 discarded warmup per scenario, with seed 0 and 256-byte payloads. The driver validates actual product imports and comparable configuration/driver/environment identities. Reports include correctness, sample counts, API p50/p95, workload-specific throughput, lifecycle timings and retained storage. These are authenticated loopback HTTP measurements, not isolated broker execution, native receipt or model-response latency.

Timing comparisons are report-only; head workload correctness failures still fail CI. `performance-python-3.12` retains the produced reports, comparison and CI context, including source hashes and the full event base/head identities. A missing/unavailable product baseline is explicitly head-only with no comparison, never an inferred improvement. A failing historical baseline retains its failed evidence and exit status while the required head measurement still runs; it cannot authorize a timing comparison or block a PR that fixes that correctness failure. The fixed base-then-head ordering and small samples can contain host/order effects; establish repeated-run variance before adding a performance budget. Manual dispatch accepts bounded message/trial/worker counts (1–128, 2–12, 1–16) and an optional complete baseline commit SHA; without that SHA it reports head-only. It still runs the full contract/package/image checks.

The image check builds the broker, verifies UID 10001, bootstraps a named state volume, rejects an invalid token, sends an idempotent message, records an acknowledgement and reply, then replaces the container and checks retained state. Its port is bound to host loopback. Cleanup removes only its UUID-named container/volume, including on failure; it does not publish, authenticate to a registry, or prune global resources.

The canonical job caches pip downloads using `pyproject.toml`, which declares the build and coverage dependencies; the lint job installs its three pinned tools uncached so it cannot claim that cache key. The benchmark adds no package dependencies or CI jobs. Installation still runs each time; the cache is not an installed environment. No external Docker cache or additional shards are configured. The earlier image step took 9–15 seconds and the suite about five seconds before fixture optimization; added cache setup or runner startup could cost more than they save. Measure hosted runs before adding infrastructure. Existing concurrency cancellation drops obsolete runs for the same workflow/ref, and matrix fail-fast remains disabled so all selected runtime results are visible.

The lint job runs on every trigger without selection; it needs no product import and takes seconds. Ruff checks Python with correctness rules (`E4`, `E7`, `E9`, `F` and bugbear `B`) plus a selected set of flake8-bandit `S` rules for patterns never intended here (`exec`/`eval`, pickle/marshal, `shell=True`, unsafe YAML, disabled TLS verification, weak hashes, binding all interfaces, SQL string building), targeting Python 3.10 and configured in `pyproject.toml`; it applies no style, import-order or formatting rules. The broader Bandit-style rules (`subprocess` without a shell, `try`/`except`/`pass`) describe intended patterns in this code base and stay off; CodeQL performs the data-flow security scan. `scripts/ci/lint_checks.py` runs the pinned Ruff with the repository configuration over an unsafe and a safe sample for every selected `S` rule: each rule must report its unsafe sample and stay silent on the safe one, and a selected rule without samples (or samples without a selected rule) fails, so a Ruff upgrade cannot silently drop a check. ShellCheck checks every tracked `*.sh`/`*.bash` file, and actionlint validates the workflows and runs the same ShellCheck over each `run:` step. Shell embedded in the Dockerfile is not linted. The tools are pinned in the development-only `lint` extra (with the `tomli` backport on Python 3.10, so the calibration also runs there), separate from the `ci` coverage extra; runtime dependencies stay empty. Suppress a finding inline with its rule code only when the reported pattern is intended, for example a lambda that runs within its own loop iteration.

For a full local check:

```sh
python -m pip install '.[lint]'
ruff check .
python scripts/ci/lint_checks.py
git ls-files -z -- '*.sh' '*.bash' | xargs -0 --no-run-if-empty shellcheck
actionlint -shellcheck "$(command -v shellcheck)"
python scripts/ci/select_checks.py --event workflow_dispatch --selection-file /tmp/xordination-selection.json
PYTHONPATH=src python scripts/ci/run_contracts.py --selection /tmp/xordination-selection.json
python -m pip install '.[ci]'
PYTHONPATH=src python scripts/ci/coverage_baseline.py --selection /tmp/xordination-selection.json --output /tmp/xordination-coverage
python scripts/ci/check_package.py
docker build --tag xordination-ci .
python scripts/ci/image_smoke.py --image xordination-ci
python scripts/testing/benchmark.py --messages 8 --workers 4 --trials 3 --warmup 1 --output /tmp/xordination-performance.json
```

The coverage command is an alternative measurement run, not a required second test execution. `check_package.py` requires the declared build-system tools installed. Image execution requires Docker and remains verified by the hosted CI run when Docker is unavailable locally.

[Testing guide](testing.md) documents whole behavior journeys, planted-fault controls, native acceptance commands and benchmark interpretation. Real native acceptance remains operator-invoked with versioned evidence outside ordinary CI.

Primary references: [GitHub skipped checks](https://docs.github.com/en/pull-requests/how-tos/merge-and-close-pull-requests/troubleshooting-required-status-checks), [code-scanning merge protection](https://docs.github.com/en/code-security/concepts/code-scanning/merge-protection), [setup-python pip cache](https://github.com/actions/setup-python/blob/main/README.md), [artifact upload](https://github.com/actions/upload-artifact/blob/main/README.md), and [Docker cache backends](https://docs.docker.com/build/ci/github-actions/cache/).
