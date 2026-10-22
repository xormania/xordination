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

| Phase | Deliverables | Acceptance and evidence | Status on `master` |
| --- | --- | --- | --- |
| 1. Durable communication core | Scoped bootstrap credentials; immutable send/reply records; idempotency; non-destructive inbox; bounded wait; receipt/status/retry; generation and attempt fencing | AC-02–AC-07, AC-09; restart, conflict, unauthorized calls, ambiguity, stale callbacks | Implemented with deterministic contracts; reopened state keeps SQLite's WAL-index lock (PR #4). See [Phase 1 acceptance status](#phase-1-acceptance-status) |
| 2. Native delivery paths | Managed Codex app-server, Claude channel, Grok ACP adapters; session identity, activity, capability and failure reporting | AC-08, AC-12, AC-13; fake-native conformance tests plus independently observed wire correlations | Mechanics and failure cases implemented and exercised with owned fake-native subprocesses; the Claude doctor probes an unlisted channel flag (PR #4). Live status remains unverified |
| 3. Live compatibility | Fresh challenge, same-session retained context, each cross-harness ordered pair, same-harness pair, ongoing-work overlap, approval wait and replacement | AC-01, AC-10, AC-11; exact executable/version/configuration and independent native receipt/reply evidence | Tooling delivered; live run outstanding. Behavior journeys, operator-invoked preparation/dispatch and a read-only evidence verifier (PR #3); managed tmux launcher with owned-window cleanup (PR #4); signal stop, a separate idle round-trip budget and hashed-checkout imports (PR #5). No live run has produced versioned evidence, so AC-01, AC-10 and AC-11 remain unverified |
| 4. Entry and deployment modes | Probe attachment and resume per native API; validate local, container, and reachable protected remote topology | Record supported/unsupported/unverified modes and the evidence; never substitute a fresh session for attachment | Capability investigation remains required |
| 5. Reproducible Xorder delivery | Package/wheel and optional OCI image; exact hashes/digests; compatible config/context/profile; persistent state recipe; readiness report | AC-14; native package smoke, image/persistence/reachability smoke, then pinned release receipt | Build definitions, recipe, and CI installed-wheel and image/persistence smoke delivered; release publication/catalog enrollment after verified artifacts exist |
| 6. Consumer adoption | Independent project fixture and integration through public tools and adapter lifecycle | AC-16; show no role/work-order dependence; preserve consumer policy | Independent quick start delivered; consumer integration changes remain separately reviewable |

CI selects affected contract groups (PR #2) and lints Python, shell and workflows with Ruff, ShellCheck and actionlint (PR #6). These are development checks; they add no runtime dependency.

### Phase 1 acceptance status

“Covered” means a named deterministic test asserts the criterion at the broker or owned fake-native adapter boundary. “Partial” names the narrower assertion that exists. None of these certify a native vendor session.

| Criterion | Status | Tests |
| --- | --- | --- |
| AC-02 identical replay returns one message; changed intent conflicts | Covered | `tests/test_store.py` `test_idempotency_uses_original_intent_before_binding_resolution`, `test_concurrent_duplicate_acceptance_and_claim_have_one_owner`, `test_queue_backpressure_keeps_existing_acceptance_replay_available` |
| AC-03 accepted messages survive restart; inbox reads do not consume | Covered | `tests/test_store.py` `test_acceptance_survives_restart_and_inbox_is_not_consumption`; `tests/test_end_to_end.py` `test_two_stdio_clients_exchange_authenticated_message_and_durable_receipt` |
| AC-04 unauthorized sender, space, reply, status and acknowledgement are rejected | Covered | `tests/test_store.py` `test_authorization_binds_token_space_recipient_and_reply`, `test_ambiguous_restart_requires_explicit_sender_retry` (recipient retry is forbidden); `tests/test_server.py` `test_rejects_browser_origin_and_payload_without_exposing_identity` |
| AC-05 crash during ambiguous submission stays uncertain without replay | Covered | `tests/test_store.py` `test_ambiguous_restart_requires_explicit_sender_retry`, `test_unclean_claim_writer_restart_requires_explicit_retry`, `test_expired_claim_becomes_uncertain_without_automatic_replay`; `tests/test_adapters.py` `test_codex_process_crash_is_uncertain_and_has_no_automatic_retry`, `test_grok_prompt_crash_retains_uncertainty` |
| AC-06 explicit retry keeps identity, adds an attempt, rejects stale completion | Covered | `tests/test_store.py` `test_ambiguous_restart_requires_explicit_sender_retry`, `test_failed_retry_and_late_confirmation_preserve_attempt_history`, `test_delayed_retired_attempt_cannot_settle_recovered_generation` |
| AC-07 replaced generations cannot send, claim or acknowledge under stale authority | Covered at the broker boundary | `tests/test_store.py` `test_replacement_fences_captured_context_and_requires_recovery`, `test_stopped_participant_requires_explicit_replacement_before_recovery`, `test_delayed_retired_attempt_cannot_settle_recovered_generation`. A replacement between HTTP authentication and the store operation is not forced |
| AC-09 concurrent insertion and paginated reads skip no visible message | Partial | `tests/test_store.py` `test_cursor_retains_empty_page_position_and_handles_new_insertion`, `test_recovery_invalidates_high_water_to_cover_newly_visible_old_message`. Insertion is interleaved between pages in one thread; no test inserts from a concurrent writer during paging |

Bounded wait is exercised by `tests/test_server.py` `test_long_wait_does_not_block_send_or_ack`, `test_wait_quota_is_released_after_wake_and_store_error` and `tests/test_mcp.py` `test_wait_saturation_does_not_starve_send_and_cancellation_ends_waits`. The current limits are listed in the [README](../README.md#limits-and-backpressure).

## Tests and gates

1. Compile the supported Python source and run one shared deterministic contract suite across the CI runtime matrix.
2. Exercise authorization and generation races at the broker boundary, including retained old-generation messages and stale delivery completion.
3. Exercise ambiguous native submission using controlled protocol fixtures. Require explicit retry and preserve the original message's attempt history.
4. Exercise MCP request/response framing and concurrent wait behavior separately from model correctness.
5. Build and install the actual wheel in a directory outside the source tree. Check its identity and console entry point.
6. Build the sidecar only when packaging inputs change. A release deployment check must persist state through container recreation and connect independent clients to the same broker.
7. Execute live checks only in an authorized native environment. Record individual exchange results, session identity, submission timing, receipt/reply evidence, and unverified capabilities. An incomplete pair must not erase verified neighboring pairs.

CI grants only read access to repository contents, uses GitHub-hosted runners, and does not publish images, install vendor CLIs, change settings, or invoke paid model sessions. Do not add coverage percentages, performance thresholds, or new CI lanes without a concrete question they answer.

AC-15 wire/event ordering needs dedicated correlated evidence where concurrency affects native output. Until a live run exists, fake-native observations demonstrate implementation invariants only.

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
| What limits and retention defaults fit real usage? | Current fixed limits are documented in the [README](../README.md#limits-and-backpressure); measure independent adoption before changing them, and document retention, tombstones and idempotency boundaries |
| Are groups/broadcast needed? | Obtain a concrete consumer use case; retain per-recipient delivery history if added |

Implementation and test results must identify remaining evidence gaps directly. A gap changes the readiness report, not the stated product goal.

## Focused regression lessons

[Test lessons](test-lessons.md) maps behavioral contracts to focused regressions, identifies the native ordering cases found during implementation, and records remaining telemetry, readiness and cleanup evidence gaps. Reuse the relevant scenario and invariant while keeping consumer policy separate and avoiding duplicate test suites.

