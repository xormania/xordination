# Xorder delivery recipe

Xordination owns its implementation and release cycle. [Xorder](https://github.com/xormania/xorder) supplies discovery, pinned packaging, configuration/context, profiles, and installation receipts. This document is a recipe for enrolling a **verified release**. No Xorder catalog entry or published image identity is created by this repository's initial implementation.

## Resource composition

| Xorder resource kind | Contents | Readiness requirement |
| --- | --- | --- |
| Executable/package | Xordination wheel or selected executable distribution; Python prerequisite and console entry point | Exact verified bytes/hash and target runtime/platform |
| Optional OCI image | Broker-only sidecar from the same source revision | Verified image digest, persistence and reachability smoke |
| Configuration | Additive harness MCP entries, endpoint/space descriptors, native adapter invocation | Preserve existing settings and detect path/conflict ownership |
| Context | Tool vocabulary, idempotency, peer attribution, attempt/receipt distinction and recovery instructions | Match installed release contract |
| Profile | Pinned compatible composition for the selected environment | Actual package/service/adapter/communication evidence at separate levels |

Choose catalog identifiers and registry names only after the selected artifacts exist and are verified. Example local image names in `README.md` are local build labels, not release identities.

## URL-driven adoption

An agent given the Xorder URL should find a compact Xordination usage entry that leads to:

1. Verified release identity, artifacts and measured compatibility.
2. Required Python/platform or Docker surface; native CLI availability only for selected native adapters.
3. A shared endpoint and space descriptor, with local worktree paths as separate adapter bindings.
4. Operator bootstrap of scoped participant credentials; owned secret/configuration locations.
5. Additive MCP configuration or native adapter launch, preserving existing operator tools and native policy.
6. Package/service readiness and an optional authorized native message/reply exchange.
7. Inspect, stop, restart and explicit replacement/retry behavior with retained state.

Xorder should not install/authenticate every vendor CLI by default. Run native adapters at the actual harness location; run the broker where participating clients can reach it.

## Installation pins and ownership

Record the Xordination release/source revision, package checksum or OCI digest, compatible configuration/context revision, installed paths, and files owned by the installation. Use an exact identity for repeatable adoption and preserve pins until an intentional update. A receipt proves supplied bytes and owned files; it does not prove model ingestion or instruction compliance.

Keep participant credentials and native credentials outside repository/context artifacts. The profile may name the token environment variable, but must not embed a participant token in shared context. Bind credentials to the selected space/participant/generation. Treat the database and token-bearing bootstrap output as private state.

## Native package profile

Install the verified wheel into an isolated Python environment and use its `xordination` entry point. Create a private state directory; bootstrap a space and participants with `space`/`join`, then run:

```sh
xordination serve --state /absolute/private/broker.sqlite3 --host 127.0.0.1 --port 8765
```

Each client joins that endpoint with its own token and runs:

```sh
xordination stdio --endpoint http://127.0.0.1:8765 --token-env XORDINATION_TOKEN
```

Harness-specific configuration must resolve the actual installed executable path. A separate `stdio` process must not create a separate broker/store. Use `adapter <harness>` only for the managed native integration selected by the operator.

## Sidecar profile

Run the verified digest with a persistent `/state` mount. The current Dockerfile defaults to `/state/broker.sqlite3`, port 8765, UID/GID 10001. Expose it only on the intended network/host interface. Place bootstrap commands on the same volume; do not give ordinary remote clients direct access to the SQLite file.

Host-native clients use a reachable published host endpoint. Sibling containers use a service name on a shared network. Separate machines/sandboxes require verified ingress/egress and transport protection. Container loopback and host loopback are different endpoints.

Do not change global proxy settings to work around a local endpoint. A local broker's credentials should stay local; remote endpoints may legitimately require a scoped proxy. Xorder's networking profile must record the actual route and trusted CA choices without weakening native approvals or token scope.

## Readiness and publication gate

| Check | Required output |
| --- | --- |
| Package | Correct installed identity and writable persistent state |
| Service | Shared broker accepts authenticated participant requests and retains records across restart |
| Adapter | Observed native identity and required installed capabilities or explicit gaps |
| Communication | Fresh challenge reaches the intended native generation and yields a correlated receipt/reply |

The last two may require paid/authenticated sessions. Run them through an authorized live verification workflow or when the operator requests live readiness. Offline install checks must not silently authenticate or launch models.

The complete native compatibility matrix belongs to Xordination's release evidence. An Xorder change needs only verification for the affected selected resource/profile, not a rebuild of every unrelated image or a repeat of every live pair.

## Update, rollback and cleanup

Before upgrading retained state, verify the source-to-target schema migration and record rollback limitations. Preserve original accepted messages and ambiguous attempts. An image rollback does not establish schema rollback compatibility.

Stop only processes and remove only configuration owned by this installation. Unrelated native sessions and existing MCP servers survive cleanup. Explicit participant replacement issues a new generation and fences old credentials; it does not silently rewrite old message recipients.

