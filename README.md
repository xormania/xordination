# Xordination

Durable communication between independently addressable AI harness sessions, reusable in any project. Authored by **xormania**.

Xordination supplies a shared broker, participant identities, a small MCP tool surface, and native delivery adapters. [Xorder](https://github.com/xormania/xorder) is the intended distribution and configuration entry point. Projects add coordination policies through the public contract.

## Status

Version **0.1.0** is an initial implementation. Python **3.10+** is required; there are no third-party runtime dependencies.

| Surface | Initial implementation | Evidence boundary |
| --- | --- | --- |
| Shared SQLite broker | Scoped credentials, durable messages, replies, receipts, attempts, explicit retry, generation fencing | Deterministic contract tests; no exactly-once model execution claim |
| External MCP clients | Independent stdio frontends use the same HTTP broker | Tool access/inbox delivery does not imply unsolicited native wake-up |
| Native adapters | Managed Codex, Claude Code, and Grok Build integration paths derived from the PoC | Native interfaces remain experimental until a versioned live exchange passes |
| Packaging | Executable Python package and optional broker sidecar | A local build is not a published Xorder resource or released image |
| Existing native sessions | Attachment/resume are product targets | No generic attachment/resume claim in this release |

The [draft specification](docs/specification.md) defines the communication contract and retains the inspected public PoC sources. The PoC's historical September 2026 live results concern its own revisions and harness versions. They do not certify the adapters in this repository. [Compatibility](docs/compatibility.md) records what must be measured before stronger claims.

## Local quick start

Install from this checkout:

```sh
python -m venv .venv
. .venv/bin/activate
python -m pip install .
xordination --version
```

Create a project space and join two independently named participants. Bootstrap operates on the broker's local database and is an operator action. Set restrictive file permissions before writing token-bearing output:

```sh
umask 077
mkdir -p .xordination
xordination space --state .xordination/broker.sqlite3 example
xordination join --state .xordination/broker.sqlite3 --space example --participant alice --harness external --output .xordination/alice.json
xordination join --state .xordination/broker.sqlite3 --space example --participant bob --harness external --output .xordination/bob.json
xordination serve --state .xordination/broker.sqlite3 --host 127.0.0.1 --port 8765
```

Each `join` creates a private connection descriptor with participant credentials and its binding generation; `--output` refuses to overwrite an existing file. Give each client only its own credential. Keep tokens and the database out of version control. An existing participant requires explicit `--replace` to create a new generation; replacement fences the old credential and does not silently transfer messages addressed to the old generation.

On POSIX systems, Xordination creates private state files and a private new state directory, and restricts existing database/sidecar files before opening SQLite. It preserves existing directory permissions. On Windows, protect the state directory and credential descriptors with appropriate OS access controls.

In a second terminal, start a stdio frontend using the participant token supplied by `join`:

```sh
export XORDINATION_TOKEN='<alice token from join>'
xordination stdio --endpoint http://127.0.0.1:8765 --token-env XORDINATION_TOKEN
```

Run another frontend for Bob with Bob's token. Both frontends connect to the **same broker**. An example MCP server entry is:

```json
{
  "mcpServers": {
    "xordination": {
      "command": "xordination",
      "args": ["stdio", "--endpoint", "http://127.0.0.1:8765", "--token-env", "XORDINATION_TOKEN"]
    }
  }
}
```

Supply `XORDINATION_TOKEN` through the harness's supported secret/environment mechanism. Add the entry to the operator's existing MCP configuration; do not replace other servers, hooks, model choices, or permissions. Configuration formats and reload behavior vary by harness.

## Communication tools

| Tool | Contract |
| --- | --- |
| `xordination_peers` | Discover peers and their current bindings/capabilities in your space |
| `xordination_send` | Durably accept a directed message; a `reply_to` links a separate reply |
| `xordination_inbox` | Read bounded, non-destructive pages addressed to your generation |
| `xordination_wait` | Wait for new visible messages up to a bounded deadline |
| `xordination_ack` | Record your receipt of a message addressed to you |
| `xordination_status` | Inspect an authorized message's attempts, receipts, replies, and uncertainty |
| `xordination_retry` | Explicitly authorize another eligible uncertain delivery attempt |

Use the MCP tool schemas for exact arguments. The sender comes from authenticated connection context. Include a stable idempotency key when sending; repeat that key with identical intent after a lost response. Different intent under the same key is a conflict. Broker acceptance, native submission, participant receipt, reply, and work completion are separate facts.

A peer message is attributed content. It carries no additional operator authority or tool permission. The broker never composes automatic replies.

## Native adapters

Use `--harness codex`, `claude`, or `grok` when joining a participant intended for the corresponding managed adapter. Native CLIs and their authentication remain where the operator already runs them. Inspect the local environment without invoking a model:

```sh
xordination doctor --harness codex
```

Start a managed adapter with that participant's token:

```sh
xordination adapter codex --endpoint http://127.0.0.1:8765 --token-env XORDINATION_TOKEN --project /absolute/project/path
```

Use `claude` or `grok` in place of `codex` for the other adapters. `--binary` selects an explicit executable; `--prompt` supplies initial native work. Managed launch creates a native session owned by that adapter; it does not attach to an unrelated running terminal. The explicit `--mode attach` and `--mode resume` paths return a capability gap until verified native integration is implemented; neither silently falls back to launch. Missing capabilities and native approvals must remain visible. A binary/version check alone does not prove native communication readiness.

The adapter preserves the project's available tools and operator policy. Consumer roles and work-order policies are supplied through the public contract. Proof-only tools, fixture workloads, and fixed peers remain test controls.

## Readiness report

`readiness` reports the four readiness levels separately and lists only those that passed. It never launches a native session or calls a model:

```sh
xordination readiness --state .xordination/broker.sqlite3 --endpoint http://127.0.0.1:8765 --token-env XORDINATION_TOKEN --harness codex
```

| Level | Passes when | Otherwise |
| --- | --- | --- |
| `package` | The imported module is the installed distribution at the running version, the `xordination` console entry point is declared and on `PATH`, and the `--state` location is usable. The store is never created or opened | `unverified` without `--state`; `failed` for a checkout or a mismatched installation |
| `service` | Broker health and an authenticated read-only `identity` operation succeed | `unverified` with health alone (no token); `failed` when unreachable or rejected |
| `adapter` | Each `--harness` has installed capability (`doctor`) and an observed native session identity from supplied passing evidence | `unverified` with no `--harness` or capability only; `failed` when unavailable or unsupported |
| `communication` | Every `--evidence` report from `scripts/testing/live.py verify` passed | `unverified` without evidence; `failed` when a report failed |

Evidence reports are structurally checked, including the verifier's own pass rule, but not re-verified. Unreadable or inconsistent evidence is refused. Without `--token-env`, `XORDINATION_TOKEN` is used only when set. Communication readiness does not establish application-work completion.

## Optional broker sidecar

Build the local broker-only image. It contains no vendor CLIs or vendor credentials:

```sh
docker build -t xordination:local .
docker volume create xordination-state
docker run --rm --name xordination -p 127.0.0.1:8765:8765 -v xordination-state:/state xordination:local
```

Initialize the persistent store and join participants with the same image, for example:

```sh
docker run --rm -v xordination-state:/state xordination:local space --state /state/broker.sqlite3 example
docker run --rm -v xordination-state:/state xordination:local join --state /state/broker.sqlite3 --space example --participant alice --harness external
```

The image runs as UID/GID `10001`. A bind mount must be writable by that identity; Docker-managed named volumes use the image's initialized `/state` permissions. Recreating a container with the same volume preserves accepted messages. Host clients use the published loopback endpoint. Container clients on a shared Docker network must use the broker's reachable service name, not their own `127.0.0.1`.

The HTTP endpoint authenticates participant requests. Use transport protection for connections crossing an untrusted network. Remote reachability and protected ingress depend on the actual deployment and are not established by a local container test. See the [Xorder delivery recipe](docs/xorder.md).

## Development and release evidence

```sh
PYTHONPATH=src python -m unittest discover -s tests -v
python -m compileall -q src tests
python -m pip wheel --no-deps --wheel-dir dist .
```

CI selects complete contract groups from the tested change and runs them on Python 3.10, 3.12, and 3.14. Whole behavior journeys cover directed exchanges, duplicate acceptance, replacement, restart/retry and held native protocol work. Required skips and unexercised cases cannot pass. Python 3.12 measures branch/thread/subprocess coverage and validates a wheel rebuilt from the source archive in a clean installation. Selected broker inputs also produce repeatable base/head HTTP performance reports on the same runner, with correctness gates and report-only timings. The other runtimes retain source contracts and CLI checks. Unknown changes and manual dispatch select full validation; the image check exercises authenticated durable state across container replacement.

See [testing guide](docs/testing.md) for deterministic behavior, planted-fault controls, performance commands and opt-in native acceptance; [CI checks](docs/ci.md) for selection and evidence; and [coverage baseline](docs/coverage-baseline.md) for measured gaps. Ordinary CI does not authenticate or charge native model sessions. Live acceptance requires real operator-selected harnesses and independently correlated native observations; no current vendor pass is claimed merely because its validator tests pass.

The repository's existing `master` rules also require a Python CodeQL scan. Its workflow can upload security results and has read-only access to source contents.

[Implementation plan](docs/implementation-plan.md) maps the initial changes and remaining verification to the specification's acceptance criteria. A release must publish its actual compatibility matrix and retain unknown results as unknown. State-schema upgrade and rollback claims require migration evidence.

## License and sources

Xordination uses the [Apache License 2.0](LICENSE). [NOTICE](NOTICE) preserves upstream licensing and source provenance. Source URLs retain their repository owners; authored project attribution is **xormania**.

