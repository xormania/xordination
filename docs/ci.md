# CI checks

CI starts on every pull request to `master`, push to `master`, and manual dispatch. It selects checks before Python setup or dependency installation. It does not use workflow path filters: a skipped workflow can leave a required check pending. CodeQL continues to perform its actual Python scan independently on every existing trigger.

The selector compares the complete event base with the tested commit. Pull requests use the checked-out merge tree and require that the advertised base is its ancestor; pushes use `before`. Both rename paths and deletions are retained using NUL-delimited Git records. Missing commits, malformed event/diff data, unknown paths, Git failures, and manual dispatch select everything. Selection publication failure makes the job fail. Mixed changes union their dependencies.

| Changed input | Contract groups | Package | Image |
| --- | --- | --- | --- |
| `store.py` | store, adapters, server, cli, end_to_end | yes | yes |
| `adapters.py` | adapters, cli | yes | yes |
| `rpc.py` | adapters | yes | yes |
| `mcp.py` | adapters, mcp, cli, end_to_end | yes | yes |
| `server.py` | server, cli, end_to_end | yes | yes |
| `client.py`, `cli.py` | adapters, mcp, server, cli, end_to_end | yes | yes |
| `__init__.py`, `__main__.py`, unknown runtime input | all | yes | yes |
| Existing registered test module | its group | yes | no |
| New/shared test input, CI helper, workflow, unknown path | all | yes | yes |
| `pyproject.toml` | all | yes | yes |
| `MANIFEST.in` | all | yes | no |
| `README.md`, `LICENSE`, `NOTICE`, `Dockerfile` | none | yes | yes |
| `docs/*.md`, `AGENTS.md` | none | yes | no |
| `.dockerignore` | none | no | yes |

The map includes lazy imports from MCP into adapters, public CLI dispatch, and the real stdio subprocess in the end-to-end tests. A full selection discovers every `test*.py`, including new modules. An absent or empty registered selected module is an error. Update the map when dependencies change; uncertainty should retain the full fallback.

The same selected groups run on Python 3.10, 3.12, and 3.14. The existing three-job graph is retained; documentation-only changes skip Python setup entirely in the 3.10/3.14 jobs. Those jobs validate source imports through the public CLI when contracts run. They no longer rebuild/install the same wheel. Python 3.12 builds one source archive, checks its included runtime/docs/tests/CI inputs, rebuilds a wheel from that archive, checks license notices, and exercises a fresh installation outside the checkout. The installed smoke explicitly rejects checkout imports and checks send/inbox/ack/status behavior. This trades repeated installed-artifact evidence on the other runtimes for the canonical installation plus their source contracts and CLI smoke.

Python 3.12 runs the selected suite once under branch/thread/subprocess coverage and uploads the report. Each report names the selected groups and whether it is full; partial reports are not whole-suite regressions. Manual dispatch refreshes the full baseline. See [coverage baseline](coverage-baseline.md) for measured gaps and process limitations. Coverage is a development-only `ci` extra; runtime dependencies stay empty. There is no percentage threshold.

The image check builds the broker, verifies UID 10001, bootstraps a named state volume, rejects an invalid token, sends an idempotent message, records an acknowledgement and reply, then replaces the container and checks retained state. Its port is bound to host loopback. Cleanup removes only its UUID-named container/volume, including on failure; it does not publish, authenticate to a registry, or prune global resources.

The canonical job caches pip downloads using `pyproject.toml`, which declares the build and measurement dependencies. It still runs installation each time; the cache is not an installed environment. No external Docker cache or additional shards are configured. The earlier image step took 9–15 seconds and the suite about five seconds before fixture optimization; added cache setup or runner startup could cost more than they save. Measure the new hosted runs before adding infrastructure. Existing concurrency cancellation drops obsolete runs for the same workflow/ref, and matrix fail-fast remains disabled so all selected runtime results are visible.

For a full local check:

```sh
python scripts/ci/select_checks.py --event workflow_dispatch --selection-file /tmp/xordination-selection.json
PYTHONPATH=src python scripts/ci/run_contracts.py --selection /tmp/xordination-selection.json
python -m pip install '.[ci]'
PYTHONPATH=src python scripts/ci/coverage_baseline.py --selection /tmp/xordination-selection.json --output /tmp/xordination-coverage
python scripts/ci/check_package.py
docker build --tag xordination-ci .
python scripts/ci/image_smoke.py --image xordination-ci
```

The coverage command is an alternative measurement run, not a required second test execution. `check_package.py` requires the declared build-system tools installed. Image execution requires Docker and remains verified by the hosted CI run when Docker is unavailable locally.

Primary references: [GitHub skipped checks](https://docs.github.com/en/pull-requests/how-tos/merge-and-close-pull-requests/troubleshooting-required-status-checks), [code-scanning merge protection](https://docs.github.com/en/code-security/concepts/code-scanning/merge-protection), [setup-python pip cache](https://github.com/actions/setup-python/blob/main/README.md), [artifact upload](https://github.com/actions/upload-artifact/blob/main/README.md), and [Docker cache backends](https://docs.docker.com/build/ci/github-actions/cache/).
