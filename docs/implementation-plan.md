# Xordination implementation and verification plan

**Owner:** xormania  
**Baseline:** draft specification dated 2026-10-10  
**Initial implementation:** 0.1.0, Python 3.10+ standard library, one SQLite authority

The [specification](specification.md) defines the intended capability and preserves source evidence. This plan makes that direction executable without treating historical tests as certification of this implementation. Owner direction may revise the plan or decisions.

## Initial architectural decisions

| Decision | Implementation direction | Reason |
| --- | --- | --- |
| Runtime | Python 3.10+, no third-party runtime dependencies | Retain the PoC's interface knowledge and keep package adoption small |
| Durable authority | SQLite owned by the shared broker; remote clients use its API | Transactional acceptance, identity, attempts, receipts, and restart reconstruction |
| MCP transport | Per-client stdio frontend connects to a shared authenticated HTTP broker | Works with harness MCP configuration without isolated per-process mailboxes |
| Identity | Space, stable participant, binding generation, observed native identity | Multiple sessions of one harness remain distinct; stale credentials are fenced |
| Native delivery | Vendor-specific managed adapters with explicit capability observations | MCP tool registration alone does not establish native wake-up |
| Distribution | Native executable package plus optional broker-only sidecar | Avoid rebuilding toolchain images or installing all vendor CLIs everywhere |
| Consumer policy | Opaque bounded metadata and public communication contract | Each project retains its own roles, work orders, tools, and approval semantics |

These decisions resolve implementation choices for the initial change. They do not narrow existing-session attachment, native resume, remote deployment, or additional harness support out of the product goal.

## Delivery sequence

| Phase | Deliverables | Acceptance and evidence | Initial change |
| --- | --- | --- | --- |
| 1. Durable communication core | Scoped bootstrap credentials; immutable send/reply records; idempotency; non-destructive inbox; bounded wait; receipt/status/retry; generation and attempt fencing | AC-02–AC-07, AC-09; restart, conflict, unauthorized calls, ambiguity, stale callbacks | Implement and verify deterministic contracts |
| 2. Native delivery paths | Managed Codex app-server, Claude channel, Grok ACP adapters; session identity, activity, capability and failure reporting | AC-08, AC-12, AC-13; fake-native conformance tests plus independently observed wire correlations | Implement mechanics and failure cases; live status remains unverified |
| 3. Live compatibility | Fresh challenge, same-session retained context, each cross-harness ordered pair, same-harness pair, ongoing-work overlap, approval wait and replacement | AC-01, AC-10, AC-11; exact executable/version/configuration and independent native receipt/reply evidence | Opt-in follow-up; requires available authenticated native environments |
| 4. Entry and deployment modes | Probe attachment and resume per native API; validate local, container, and reachable protected remote topology | Record supported/unsupported/unverified modes and the evidence; never substitute a fresh session for attachment | Capability investigation remains required |
| 5. Reproducible Xorder delivery | Package/wheel and optional OCI image; exact hashes/digests; compatible config/context/profile; persistent state recipe; readiness report | AC-14; native package smoke, image/persistence/reachability smoke, then pinned release receipt | Build definitions and recipe now; release publication/catalog enrollment after verified artifacts exist |
| 6. Consumer adoption | Independent project fixture and integration through public tools and adapter lifecycle | AC-16; show no role/work-order dependence; preserve consumer policy | Independent quick start now; consumer integration changes remain separately reviewable |

## Tests and gates

1. Compile the supported Python source and run one shared deterministic contract suite across the CI runtime matrix.
2. Exercise authorization and generation races at the broker boundary, including retained old-generation messages and stale delivery completion.
3. Exercise ambiguous native submission using controlled protocol fixtures. Require explicit retry and preserve the original message's attempt history.
4. Exercise MCP request/response framing and concurrent wait behavior separately from model correctness.
5. Build and install the actual wheel in a directory outside the source tree. Check its identity and console entry point.
6. Build the sidecar only when packaging inputs change. A release deployment check must persist state through container recreation and connect independent clients to the same broker.
7. Execute live checks only in an authorized native environment. Record individual exchange results, session identity, submission timing, receipt/reply evidence, and unverified capabilities. An incomplete pair must not erase verified neighboring pairs.

CI grants only read access to repository contents, uses GitHub-hosted runners, and does not publish images, install vendor CLIs, change settings, or invoke paid model sessions. Do not add coverage percentages, performance thresholds, or new CI lanes without a concrete question they answer.

AC-15 wire/event ordering is covered locally by the opt-in diagnostic trace: concurrent MCP stdio output and a scripted native RPC process are compared against the persisted trace sequence. Until a live run exists, fake-native observations demonstrate implementation invariants only; real vendor traces need their own correlated evidence.

## Before the first advertised compatible release

- Capture the exact source commit, package hashes/image digest, Python/platform identity, installed harness identity/version and relevant configuration fingerprint.
- Establish a retained-state migration/rollback policy and test any schema migration before instructing users to upgrade an existing store.
- Publish the native capability matrix and distinguish package, service, adapter, and communication readiness.
- Complete a two-client message/reply smoke in each published deployment profile; exercise replacement and restart without silently replaying ambiguity.
- Measure broker acceptance latency, queue burst behavior, bounded waits, memory/CPU use, and recovery separately from model response latency before setting budgets.
- Create Xorder resource entries only with verified bytes, exact identities, and matching config/context revisions. Preserve existing pins unless an operator intentionally updates them.

## Open evidence and owner decisions

| Question | Next concrete action |
| --- | --- |
| Which installed harness versions pass native idle and ongoing-work delivery? | Run the versioned live matrix; publish per-capability results |
| Can an adapter attach to an existing session or resume one? | Inspect and exercise the installed native API without creating a substitute session |
| Which OS/architecture combinations belong in the first release? | Build and run packaging/deployment smoke on requested environments |
| How should departed-session messages transfer to a replacement? | Confirm the operator experience and implement explicit generation handover with cursor resynchronization |
| Which endpoint/ingress owns cross-sandbox connectivity? | Validate real reachability, protected transport, credential scope and scoped proxy routing |
| What limits and retention defaults fit real usage? | Measure independent adoption and document discoverable limits, tombstones and idempotency boundaries |
| Are groups/broadcast needed? | Obtain a concrete consumer use case; retain per-recipient delivery history if added |

Implementation and test results must identify remaining evidence gaps directly. A gap changes the readiness report, not the stated product goal.

## Focused regression lessons

[Test lessons](test-lessons.md) maps behavioral contracts to focused regressions, identifies the native ordering cases found during implementation, and records remaining telemetry, readiness and cleanup evidence gaps. Reuse the relevant scenario and invariant while keeping consumer policy separate and avoiding duplicate test suites.

