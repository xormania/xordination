# Behavior, live acceptance and performance testing

**Owner:** xormania

The [multi-harness-proof PoC](https://github.com/xormania/multi-harness-proof) supplies the reference model: process-level fault scenarios, fresh challenge identity, distinct submission/receipt/reply evidence, ongoing-work controls, and independent verification. Its [testing guide](https://github.com/xormania/multi-harness-proof/blob/master/TESTING.md) and [experiment controls](https://github.com/xormania/multi-harness-proof/blob/master/EXPERIMENTS.md) describe historical PoC tests. Those results do not certify Xordination or a current vendor executable.

## Evidence layers

| Layer | What runs | What a pass establishes |
| --- | --- | --- |
| Focused contracts | Broker, MCP, HTTP, CLI and scripted native protocol tests | The asserted implementation invariant for the exercised schedule |
| Behavior journeys | Independent stdio clients, real HTTP/SQLite boundaries and an owned scripted native process | End-to-end protocol behavior with run, message, generation and attempt correlations |
| Opt-in live acceptance | Operator-selected real native adapters and separately captured native tool observations | The requested directed pairs/scenarios correlated to captured native identities and tool calls; versions/settings remain operator observations |
| Performance | Authenticated loopback HTTP workloads against selected product source | Correctness plus measured API latency, throughput, lifecycle costs and retained storage for that workload |

Ordinary CI runs the deterministic layers and performance-driver correctness tests. It does not install vendor CLIs, authenticate native sessions, invoke models or infer model behavior from scripted executables. Native submission, participant receipt, agent action and work completion remain separate facts.

## Shared fixtures and whole journeys

[Shared helpers](../tests/helpers.py) own fresh stores, broker servers and stdio processes. [Native fixtures](../tests/native_fixtures.py) reuse immutable scripted wire behavior; they do not share mutable sessions or databases. Readiness barriers replace startup sleeps. Cleanup releases holds and closes only owned resources even when a case fails.

[Behavior journeys](../tests/behavior.py) cover every directed pair among three configurable participant labels by default, then exercise idempotent duplicate acceptance, generation replacement with explicit recovery, broker restart with uncertain delivery and explicit retry, and native submission during an unfinished held tool. Harness names remain attributes rather than participant identities. The busy fixture establishes protocol submission during a held workload; it does not establish real model ingestion or simultaneous token generation.

```sh
PYTHONPATH=src python tests/behavior.py --output /tmp/xordination-behavior.json
PYTHONPATH=src python -m unittest discover -s tests -p test_behavior.py -v
```

The runner freezes an expected run and case manifest separately from produced observations. Its verifier checks fresh challenge content, original and delivery generations, message/attempt identity, authenticated receipt, linked reply, explicit retry authority and stale-callback fencing. A missing required case is unexercised and prevents a pass. Malformed or failing cases retain neighboring evidence rather than erasing it. Recorded source hashes must remain unchanged during the run.

Planted faults must fail the intended assertion after a passing control. Setup failures, unrelated exceptions, timeouts and skipped cases are not substitutes for that red. Five production mutations deliberately remove a durable receipt, unlink a reply, bypass message idempotency, automatically replay uncertain work, or accept a stale callback; each must trigger its intended oracle assertion. Separate altered-artifact tests reject wrong run/generation/message identity, wrong receipt evidence level, stale reply content, malformed/duplicate manifests and missing required cases. This bounded catalog demonstrates those rejection paths; it is not a claim of universal equivalence or exhaustive failure coverage.

## Opt-in native acceptance

The [live runner](../scripts/testing/live.py) prepares a private, fresh run directory, freezes participant generations/native identities before the cases, sends exact tasks through the broker, then verifies a read-only store snapshot against the frozen manifest and native tool observations. It never substitutes a protocol fixture for a required native observation.

Create a configuration file outside version control. This minimal example requires both directed pairs; `alpha` and `beta` are independent participant names:

```json
{
  "participants": [
    {"name": "alpha", "harness": "codex"},
    {"name": "beta", "harness": "grok"}
  ],
  "scenarios": ["idle", "busy"]
}
```

Record actual operator-observed executable versions in each participant's optional `version` field. Optional `model` and `reasoning` fields record requested settings; they do not replace native configuration or establish independently observed effective settings. Missing identity information remains unknown. `xordination doctor --harness codex` and the equivalent harness probe inspect installed binaries without invoking a model.

```sh
PYTHONPATH=src python scripts/testing/live.py prepare \
  --config /tmp/xordination-live-config.json --output /tmp/xordination-live-run
PYTHONPATH=src python -m xordination serve --state /tmp/xordination-live-run/broker.sqlite3 --host 127.0.0.1 --port 8765
```

With that broker running, launch each chosen real adapter in its own terminal. These commands invoke authenticated native sessions and can incur provider charges:

```sh
XORDINATION_ENDPOINT=http://127.0.0.1:8765 PYTHONPATH=src python scripts/testing/live.py adapter \
  --run /tmp/xordination-live-run --participant alpha --project "$PWD"
XORDINATION_ENDPOINT=http://127.0.0.1:8765 PYTHONPATH=src python scripts/testing/live.py adapter \
  --run /tmp/xordination-live-run --participant beta --project "$PWD"
```

After both adapters report native bindings, freeze the run, dispatch its source tasks and verify after the agents have exchanged receipts and replies:

```sh
PYTHONPATH=src python scripts/testing/live.py capture --run /tmp/xordination-live-run
XORDINATION_ENDPOINT=http://127.0.0.1:8765 PYTHONPATH=src python scripts/testing/live.py send --run /tmp/xordination-live-run --phase-timeout 30
PYTHONPATH=src python scripts/testing/live.py verify --run /tmp/xordination-live-run
```

Use a fresh directory for every attempt. Retain the run privately: it includes scoped credentials and native observations. The verifier requires each declared directed pair/scenario; missing cases prevent a pass while verified neighboring cases remain reported. It checks both exchange legs, including native submission, broker receipt and correlated native acknowledgement of the reply. Each verification produces a new immutable report so a rerun retains earlier failed evidence. The optional `pairs` list selects the expressly requested matrix; unused configured participants need not launch or qualify a required pair.

Dispatch completes the required idle roundtrips before priming busy work. For each busy case, the controller asks the recipient to execute an exact `xordination_wait` with its current scoped cursor. A test-only hold keeps that actual native request unfinished. The controller waits for both held-request readiness and a separately captured native call ID before asking the source to send its challenge. It records the committed exact message/attempt submission while the hold remains open, releases the hold in cleanup, and waits for its closure before the next case. Later correlated acknowledgement/reply establishes handling; dispatch alone does not. Gate artifacts retain arm, hold readiness, native readiness, exact submission, release and closure; failures retain their stage/reason. The verifier requires their run/case/generation/native/call correlations and monotonic order. An activity flag or an arbitrary sleep cannot replace these observations.

`--phase-timeout` defaults to 30 seconds and accepts a finite positive value up to 300 seconds. A busy pass establishes native submission during an unfinished native request followed by correlated handling. It does not certify simultaneous model computation, token-generation interruption or the PoC's separate retained-private-context proof.

Codex holds fulfillment in its observed native tool handler. Grok uses an owned test-only `live.py stdio` frontend around the existing MCP server, retaining its public tool schemas and normal tool behavior while holding the selected wait. Both retain native tool observations. These controls are acceptance tooling; the broker's normal wait contract is unchanged.

Claude uses the production managed adapter with its prepared participant credential and the test-only `INITIAL_PROMPT` from `live.py` supplied through `--prompt`. It additionally requires an additive `PreToolUse` hook invoking `python /absolute/path/scripts/testing/live.py observe-claude --run /absolute/run --participant <name>` in that actual Claude session. The hook holds the selected native request before its normal permission/tool flow. Configure its timeout above twice `--phase-timeout`, allowing additional cleanup time, so the harness does not terminate the hold prematurely. Preserve existing hooks, model choices and permissions; hook configuration/reload depends on the installed harness. A missing observer or a prematurely closed hold is unverified, never a fallback pass. The run-local capture lock currently requires a POSIX host.

Observation failures write bounded diagnostics and invalidate required acceptance evidence. The passive hook returns empty output and zero exit on capture failure; it issues no permission decision, rewrites no tool arguments and does not turn a recording error into denial of an otherwise permitted native tool. Normal harness permissions remain authoritative.

Native IDs are adapter-reported; configured versions/settings are operator observations. Observer files are correlated run-local evidence, not attestation against hostile local editing. A deterministic validator test is evidence that the oracle accepts/rejects its fixtures, not evidence that an actual vendor run passed. Publish a live compatibility claim only with the actual retained run and its precise evidence boundary.

## Performance measurements

The [benchmark driver](../scripts/testing/benchmark.py) uses the same owned broker fixture with fresh state for every trial. It includes a serial question/reply journey with both receipts and linked status, and concurrent independent peer pairs that enqueue and drain a bounded backlog. Every measured trial validates content, generation, message order, distinct receipts and an empty unacknowledged queue. Correctness failures return nonzero even under Python optimization.

```sh
python scripts/testing/benchmark.py --messages 8 --workers 4 --trials 3 --warmup 1 \
  --payload-bytes 256 --seed 0 --output /tmp/xordination-performance.json
```

Operation timings use a monotonic clock and include client serialization, loopback HTTP, broker work and SQLite work. Reports retain individual samples, nearest-rank p50/p95 with sample counts, workload-specific throughput, separate setup/steady/cleanup duration and retained SQLite/WAL storage. A serial work unit is a two-way question/reply; a concurrent work unit is one-way send/read/receipt. Comparing their throughput without those units would be misleading. Storage observations are not process-memory measurements.

Warmups run the same correctness checks and are discarded from timing statistics. The manifest records configuration, seed, product and driver/helper hashes, actual imported product paths, Python/platform/SQLite identity and unmeasured phases. Native submission, native receipt, model response, remote networking and broker process RSS are unmeasured by this driver.

For a comparison, run the same current driver against both source trees on the same runner, with identical arguments and one shared measurement-session identifier:

```sh
python scripts/testing/benchmark.py --source /tmp/base/src --session-id local-pair \
  --messages 8 --workers 4 --trials 3 --warmup 1 --output /tmp/base.json
python scripts/testing/benchmark.py --source ./src --session-id local-pair \
  --messages 8 --workers 4 --trials 3 --warmup 1 --output /tmp/head.json
python scripts/testing/benchmark.py --compare-base /tmp/base.json --compare-head /tmp/head.json \
  --output /tmp/comparison.json
```

Comparison rejects mismatched configurations, driver hashes, environments and measurement-session identities. The identifier is a correlation control; orchestration establishes use of the same runner. A pair of small samples can show variation without establishing a regression or improvement. CI records timing changes without a latency gate; establish repeated-run variance and a concrete budget before introducing one. Keep model/provider timing in a separately evidenced measurement rather than mixing it into broker API cost.

## Required execution and CI selection

The selected-suite runner reports collection, case starts, executed cases, skips and unexercised cases separately. A skipped required case, a class fixture failure that prevents required execution, or an `expectedFailure` marker cannot authorize a pass. Intentional fault scenarios use an explicit expected-reason assertion inside a passing test.

See [CI checks](ci.md) for the dependency map and same-runner base/head measurement. Unknown inputs, new/shared fixtures and incomplete Git changes select all checks. Behavior, performance and live-evidence validator tests run on all supported CI Python versions when their dependencies change. Performance timing runs only on canonical Python 3.12 when broker or benchmark inputs are selected.
