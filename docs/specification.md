# Xordination — Reusable Multi-Harness Communication Specification

- **Status:** Draft 0.1 for xor's review
- **Date:** 2026-10-10
- **Project:** https://github.com/xormania/xordination
- **Distribution:** Xorder
- **Purpose:** Define a reusable communication service using the demonstrated capabilities of multi-harness-proof and explicit contracts for durable delivery, session lifecycle, and operator control.

This document distinguishes **observed source behavior**, **historical live evidence**, and **proposed requirements**. Requirements below describe the proposed Xordination product; they do not claim that Xordination already implements them. xor's subsequent direction takes precedence. The owner has selected Python 3.10+ as the implementation language for now ([§14](#14-decisions-still-open)); several deployment choices remain recommendations or open decisions.

## 1. Product objective

Xordination enables independent AI harness sessions to discover peers, exchange messages and replies, and inspect delivery status while retaining their native sessions and context. A project can adopt it through a small configuration and an Xorder-distributed package or image.

The initial adapter targets are Codex, Claude Code, and Grok Build because the PoC demonstrated communication between those harnesses. The core protocol must allow additional harnesses and multiple simultaneous sessions of the same harness.

The product should support:

1. Two or more sessions collaborating in one project, including different worktrees.
2. Sessions asking one another questions and sharing results during ongoing work.
3. Reliable message retention across broker or adapter restarts.
4. Explicit session replacement without confusing old and new participants.
5. Local processes, container deployments, and connections across reachable environments.
6. Integration into coordinators without requiring their workflow models.
7. Discovery, installation guidance, exact version selection, and readiness checks through Xorder.

**Success means a message reaches the intended native session and its receipt or reply can be evidenced.** A listening process, successful MCP handshake, or queue insertion establishes only part of that result.

### 1.1 Example user experience

An operator enables Xordination for a project through Xorder. Each participating harness joins the same collaboration space with its own authenticated identity. An agent can discover peers, send a question to a specific session, continue working, receive a reply through its supported native path, and inspect the exchange.

A project can retain its existing tools, models, approval settings, and working conventions. A coordinator can add assignments, dependencies, or roles above this service.

### 1.2 Boundaries

| Xordination owns | Supplied by consumers or adjacent systems |
| --- | --- |
| Peer discovery and addressing | Team structures, CO/XO roles, assignment policies |
| Durable message and reply records | Work orders, job dependencies, completion gates |
| Native session bindings and delivery adapters | Model selection strategy and vendor fallback policy |
| Delivery attempts, receipts, uncertainty, and retry controls | Meaning of a task result or approval |
| Capability observations and communication diagnostics | Code editing, project testing, GitHub workflow management |
| Deployment and adapter lifecycle needed for communication | Long-term knowledge synthesis and recovery of lost model context |

Messages may carry opaque consumer metadata such as a work-order reference. The core must not require a consumer-specific schema to route them.

Xordination may preserve messages after a session dies. Reconstructing that session's reasoning or full conversation is a separate capability and must not be implied by mailbox recovery.

## 2. Evidence and source baseline

Source inspection was performed on 2026-10-10. No new live harness experiment was run while preparing this document.

| Repository | Inspected revision | Relevance |
| --- | --- | --- |
| multi-harness-proof | `ad294398501ed119a436db54eca784be2fb10e76` | Persistent native adapters, relay/MCP surface, compatibility probes, independent verification |
| Xorder, `master` | `3219d1c41dd7b60f70542b02865d9ce2748dd348` | Resource kinds, profiles, exact identities, publication and readiness contracts |
| Xordination, `master` | `ff520b68191c869171af16728ee2aa482aea6055` | Initial repository; no communication implementation at inspection |

### 2.1 What the PoC established

The repository records a successful live run on **2026-09-20 UTC**, using version **0.4.1**. It reports **15/15 message checks**, **9/9 work batches**, and **69/69 matched native tool observations**. The live-tested code revision was `a944f31b7527807d000da985c546e8b5bc9ff476`; the later inspected head includes documentation changes. [P1], [P2]

The evidence concerns persistent Codex, Claude Code, and Grok Build sessions launched for the experiment. Its checks combine fresh challenges, retained session context, replies, and independently observed native tool calls. [P2], [P3]

Preserve these limits:

- Busy delivery measures submission overlapping an outstanding proof-tool call. In the recorded successful run, replies followed release of that held call. This does not establish token-generation interruption or interruption of arbitrary native tools.
- Work overlap means the recipient still had unfinished fixture work. It does not establish simultaneous model computation.
- The launcher creates fresh persistent sessions. Attaching to an unrelated, already-running terminal session was outside that implementation.
- Native interface support is version-sensitive.
- The relay's queues are in memory. An event log alone does not provide durable queue recovery.
- Loopback binding and peer credentials do not isolate mutually hostile processes sharing an operating-system user. [P3], [P4]

These limits identify work for Xordination; they are not permanent restrictions on its intended capabilities.

### 2.2 Lessons to carry forward

| Observed lesson | Proposed Xordination response |
| --- | --- |
| Queue insertion, native submission, and agent handling are separate observations. | Represent them separately in the public contract and diagnostics. |
| Models can claim successful communication without sufficient native evidence. | Record authenticated identity and native transport evidence independently of message prose. |
| A transport crash can leave native submission unresolved. | Retain uncertainty and make potentially duplicating retries explicit. |
| A stable member can outlive several native sessions. | Separate participant identity, native session identity, and binding generation. |
| Old callbacks and transcript entries can be attributed to replacement sessions. | Bind events and attempts to the captured generation and native identity. |
| Startup connection readiness does not prove message ingestion. | Provide separate connection, adapter, and round-trip readiness results. |
| Native capabilities and tool catalogs change. | Probe installed behavior and invalidate stale observations. |
| Proof restrictions serve the experiment's controlled workload. | Extract reusable contracts and adapters; keep proof and consumer policy outside the communication core. |
| Operator settings and native approvals affect real execution. | Preserve operator-owned configuration and report relevant blockers. |
| Multiple stores and overlapping lifecycle paths increase integration risk. | Use one authoritative broker store and clearly assigned lifecycle owners. |

The PoC supports the distinctions between submission and handling, readiness evidence, native capabilities, and experiment-specific policy. The durable delivery, generation fencing, and lifecycle responses are proposed product requirements. [P3], [P4], [P5], [P6]

## 3. Proposed architecture

Use a small shared broker with a stable communication contract and replaceable native adapters.

| Component | Responsibility | Boundary |
| --- | --- | --- |
| Communication core | Addressing, authorization, message validation, idempotency, receipts, delivery state | Independent of harness vendors and consumer workflows |
| Durable store | Messages, attempts, identity bindings, cursors, capability observations, event metadata | One authoritative write path |
| MCP frontend | Expose communication tools to clients | Does not equate MCP transport identity with native session identity |
| Native adapter | Launch/connect as supported, observe identity and lifecycle, submit messages through the native interface | Encapsulates vendor/version differences |
| Runtime management interface | Start, join, inspect, stop, replace, and recover broker/adapter instances | Operator or authorized coordinator controls lifecycle |
| Xorder resources | Package, configure, pin, document, and verify the selected release | Xordination remains independently versioned |
| Consumer integration | Attach project-specific metadata and coordination rules | Uses the public service contract |

These are logical boundaries. They do not require a separate process, package, or microservice for each row.

### 3.1 Shared service discovery

Per-harness stdio MCP processes must rendezvous with the same broker for the chosen collaboration space. They must not independently create isolated communication state.

A broker may host multiple authorized spaces. A space identifies a particular collaboration context within a project. Different worktree paths may participate in the same space; two unrelated teams working in the same repository may use different spaces.

Endpoint discovery should be deterministic from explicit project configuration or a supplied connection descriptor. Filesystem paths are local bindings, not globally meaningful participant identities.

### 3.2 Proposed initial storage

**Recommendation:** SQLite owned by one broker process for the first implementation, with transactional updates and a documented schema migration policy. Keep storage replaceable behind a narrow interface.

Container deployments persist the store outside the disposable container filesystem. Remote clients use the broker API rather than sharing the database file over a network filesystem.

The implementation language is Python 3.10+ for now ([§14](#14-decisions-still-open)); the SQLite driver, packaging method, and remote transport implementation remain open. These choices must preserve the contract rather than changing its guarantees.

## 4. Identity, scope, and lifecycle

### 4.1 Domain terms

| Term | Meaning |
| --- | --- |
| Project | An explicitly configured collaboration boundary; may include several worktrees |
| Space | A communication namespace for a particular collaboration within a project |
| Participant | A stable logical identity authorized to communicate in a space |
| Native session | The actual thread/session owned by a harness |
| Binding generation | An opaque incarnation identifier for a participant's current native-session binding |
| Connection | One MCP or adapter connection; reconnecting does not inherently create a new native session |
| Message | An immutable logical communication record |
| Delivery attempt | One effort to submit a message to a particular binding |
| Receipt | An authenticated observation of receipt, with its evidence level identified |
| Reply | A new message linked to an earlier message |

Harness names are attributes, not unique peer identifiers. Two Codex sessions must be independently addressable. Friendly aliases may resolve to participants, but resolution must report the actual destination and generation.

### 4.2 Binding requirements

**ID-1.** The broker assigns or validates sender identity from authenticated connection context. A caller cannot impersonate another participant by supplying a `sender` field.

**ID-2.** Store requested identity separately from independently observed native identity. Report which is available. A generic external MCP client may lack native identity evidence; that must remain visible.

**ID-3.** A binding captures participant, project/space scope, generation, native identity when observed, adapter identity/version, and relevant worktree binding. Capture operation scope once and revalidate it at publication; mutable current selections or timestamps alone cannot establish continuity.

**ID-4.** Each session-bound operation carries the captured binding context. Stopped or superseded generations cannot authorize new sends, delivery claims, or acknowledgements under the replacement binding. Delayed evidence may still be recorded against its original attempt and generation, without changing the replacement binding or settling a different attempt.

**ID-5.** Replacement and in-flight operations are serialized or transactionally fenced. Old attempt results and delayed completion events cannot update a newer attempt or generation.

**ID-6.** Messages sent to a live session capture its resolved generation. Replacing that session does not silently redirect those messages to the replacement. Explicit recovery may authorize the new generation to inspect or take over pending work, preserving original identity and history.

**ID-7.** Broker restart, adapter reconnect, native-session resume, and native-session replacement are distinct operations. Reuse a binding only when continuity is established; create a new generation when the native session changes or continuity cannot be safely established.

**ID-7a.** When a live binding's harness reports a different native session than the one bound, the broker fences that binding durably rather than replacing it, and records the observed identity separately from caller-supplied capabilities, so recording it cannot exceed their size limit. Until explicit replacement, every model-facing communication operation on the binding (peers, send, inbox, wait, ack, retry, claim and recover) is refused with `replacement_required`. Identity, and status with message content withheld, remain available for diagnosis. Late attempt evidence is retained but cannot settle delivery.

**ID-8.** Process liveness, connection liveness, native activity, and agent readiness are separate facts. A heartbeat does not prove the agent can receive or respond.

**ID-9.** Shutdown and cleanup operate only on resources owned by the current Xordination instance. Record ownership of launched processes, native sessions, terminal windows, sockets, and generated files. Externally joined sessions survive broker shutdown; closing their communication connection does not authorize terminating them.

These safeguards apply independently of consumer roles or work-order models.

### 4.3 Session entry modes

Each adapter advertises and verifies the modes it supports:

| Mode | Meaning |
| --- | --- |
| Managed launch | Start a new persistent native session with the communication integration |
| Existing-session attachment | Connect to a session already running, where the native interface permits this |
| Native resume | Reopen a known native session using the harness's supported resume mechanism |
| External client join | Register an authorized MCP client with the evidence and delivery capabilities actually available |

Existing-session attachment remains a product target. Its feasibility must be checked per harness and version. An unsupported or unverified mode is a recorded capability gap; it must not be silently replaced by a fresh session.

Lifecycle helpers must preserve native authentication and user settings. Installation, communication authorization, permission to launch/resume a harness, and permission to execute project work are separate actions.

## 5. Public communication contract

Keep the model-visible surface compact. The following names are provisional; the semantics are the intended contract.

| Proposed tool | Purpose |
| --- | --- |
| `xordination_peers` | List authorized peers, current bindings, observed activity, and delivery capabilities |
| `xordination_send` | Persist a directed message; use `reply_to` for a reply |
| `xordination_inbox` | Read a bounded page of messages visible to the current participant |
| `xordination_wait` | Wait for new visible messages up to a bounded deadline, then return a normal inbox result |
| `xordination_ack` | Record an explicit recipient receipt for a message |
| `xordination_status` | Inspect an authorized message's acceptance, attempts, receipts, replies, and unresolved uncertainty |
| `xordination_retry` | Explicitly retry an eligible uncertain delivery under the configured authority policy |

Joining and native binding are bootstrap/runtime responsibilities. They must not rely solely on a model asserting an arbitrary native session ID.

### 5.1 Message envelope

The logical record includes:

- Schema version, broker-assigned message ID, space, and broker ordering cursor.
- Authenticated sender and its originating generation.
- Resolved recipient and intended generation or an explicit offline routing policy.
- Sender-supplied idempotency key.
- Content and optional content type.
- Optional `reply_to` and conversation/correlation reference.
- Optional bounded opaque consumer metadata.
- Creation time and optional expiry.
- Separate linked attempts and receipts.

Broker-owned identity and timestamps cannot be overwritten by caller metadata. Metadata remains data; the communication core must not interpret a role label as extra authority.

For first release, direct messages and replies are the primary interaction. Any group or broadcast feature should expand to explicit recipient records so that partial delivery remains observable. Fan-out is an extension decision, not an implicit property of a direct message.

### 5.2 Send, reply, and idempotency

**MSG-1.** A successful acceptance response means the message and its queue record are durably committed. Return the same logical message after a retried identical request.

**MSG-2.** Scope idempotency keys to space and authenticated sender. Reusing a key with a different recipient, generation target, content, reply reference, or other semantic payload must return a conflict.

Resolve aliases and binding generations only for a new idempotency key. Store both the caller's requested destination and the resolved destination. For an existing key, compare the original supplied semantic intent and return the original message without resolving its destination again, even if an alias or current binding has changed. Explicitly requesting a new generation is a new intent and requires a new key.

**MSG-3.** Validate reply references and authorization. A caller may link replies only to messages it is allowed to participate in. Preserve the thread's space and record the actual reply destination.

**MSG-4.** A reply is a separate message. The broker must not autonomously compose replies or initiate follow-up conversations. An adapter may start or resume a native turn to deliver an authorized message according to its verified capabilities. Receiving a reply does not itself authorize an automatic reply chain.

**MSG-5.** Acknowledgement is idempotent and recipient-authorized. A sender cannot acknowledge on behalf of the destination. Historical recovery receipts must identify the authorized recovering generation.

**MSG-6.** Inbox reads are non-destructive. Reading, submitting, acknowledging, replying, and completing application work are different events.

**MSG-7.** Cursor pagination must avoid gaps under concurrent insertion and reconnect. Return a continuation cursor and a stable resume position; reaching the end of one page must not erase the client's high-water mark.

Cursors are scoped to the participant and visibility revision. If handover or authorization makes older records newly visible, return an explicit resynchronization result with a recovery cursor/snapshot covering those records. Do not rely on a creation-time high-water mark that would skip them. Consumers deduplicate repeated records by logical message ID.

**MSG-8.** Define bounded message size, page size, wait duration, pending queue size, and retention behavior. Report explicit limit errors. Exact defaults should follow initial measurements and remain discoverable.

**MSG-9.** Ordering is broker acceptance order within the documented scope. Concurrent native execution or replies may finish out of order. No global causal or exactly-once work-execution guarantee is implied.

**MSG-10.** An inbox-only or wait-based client may join. Its delivery mode must be reported accurately; it does not satisfy a claim of unsolicited native delivery.

## 6. Delivery semantics and recovery

### 6.1 Separate acceptance, attempts, and receipts

Avoid a single overloaded `sent` or `delivered` boolean.

| Record/fact | Meaning |
| --- | --- |
| Accepted | Broker committed the logical message |
| Queued | A delivery is waiting for an eligible destination |
| Claimed | A specific adapter/generation owns a bounded attempt |
| Submitted | Native interface accepted the submission according to the adapter's documented evidence |
| Uncertain | Submission may have occurred, but its outcome cannot be established |
| Definite attempt failure | Evidence establishes failure for this attempt; include whether retry is safe |
| Receipt | The intended recipient acknowledged, or an independently observed native event establishes a specifically described receipt |
| Reply | An authorized peer produced a correlated reply |
| Expired/cancelled | A recorded policy or authorized action prevents further automatic attempts |

Attempt status and receipt status are independent dimensions. A receipt can arrive after an uncertain attempt; retain the earlier uncertainty and add the new evidence. An acknowledgement does not mean the agent accepted an assignment or completed work.

Every receipt records its observer, authenticated principal, message and attempt references where available, observed native session/generation, observation time, and evidence kind. Distinguish client acknowledgement, independently observed insertion into a native session, and a correlated native agent acknowledgement or reply. Do not infer a stronger observation from a weaker one. A client-only acknowledgement does not establish native communication readiness. Observation time is not automatically the moment content entered the model's context.

### 6.2 Retry policy

A transport crash during delivery produces `uncertain`; only an explicit sender retry requeues an uncertain, unacknowledged message.

For Xordination:

1. Retry acceptance requests with the same idempotency key after a lost broker response.
2. Automatically retry only failures known to precede native submission, or paths with verified native idempotency.
3. Preserve `uncertain` when a write, timeout, or crash could have occurred after submission.
4. Require explicit retry authorization or an explicitly configured consumer policy for ambiguous replay.
5. Keep the original logical message ID and add a new attempt ID when retrying delivery.
6. Deduplicate repeated message IDs before native submission where the adapter can do so; retain the crash window between native side effects and local recording.
7. Reject stale attempt completions. A response from attempt A cannot settle attempt B.
8. Suppress further automatic submission after an authoritative receipt, while retaining the complete history.

This contract provides durable acceptance and controlled delivery attempts. It does not promise exactly-once model execution or exactly-once external side effects.

Preserve each message's result independently. A failed exchange in a burst must not erase neighboring verified exchanges or turn an incomplete result into an all-or-nothing success claim. The PoC's earlier work-run analysis illustrates why partial evidence matters. [P2], [P9]

### 6.3 Failure and restart behavior

| Event | Required result |
| --- | --- |
| Broker fails before commit | No acceptance claim; sender may retry with the same key |
| Broker commits but response is lost | Retried request returns the committed logical message |
| Adapter disconnects before taking an attempt | Message remains pending |
| Adapter fails after claiming, before outcome is known | Preserve uncertainty unless stronger evidence proves no submission |
| Native session is replaced | Old operations are fenced; pending old-generation messages require explicit disposition |
| Recipient disconnects after acknowledgement | Receipt remains durable |
| Broker restarts | Reconstruct pending work and identities from durable state; reconcile stale claims |
| Delayed native completion arrives | Apply only to its captured attempt/session/turn context |
| Message expires while pending | Stop eligible future delivery and preserve the reason |
| Retention removes payload | Retain a documented tombstone/deduplication boundary; do not silently make old request keys reusable |

Cancellation or expiry cannot retract content already submitted to a native session. Report an in-flight outcome honestly.

## 7. Harness adapters and capability evidence

### 7.1 Source-derived adapter baseline

| Harness | Path used by the PoC | What must be revalidated |
| --- | --- | --- |
| Codex | Persistent app-server thread; incoming peer content through `turn/start.toolOutput`; PoC exposes native dynamic tools | Installed schema support, turn behavior, tool exposure, native identity, attachment/resume options |
| Claude Code | Persistent native TUI/session; MCP `notifications/claude/channel`; hooks supply identity/tool/stop evidence | Channel availability and approval, actual ingestion, hook compatibility, session lifecycle |
| Grok Build | Persistent ACP session; `session/prompt` when idle and `_x.ai/interject` when active; native tool/completion observations | Extension availability, interjection behavior, tool identity metadata, delayed completions |

These describe the inspected PoC implementation, not a claim that every current installation supports them. In particular, the PoC's Codex dynamic-tool path is not evidence that ordinary MCP registration alone supplies incoming native delivery. [P3], [P5]

### 7.2 Adapter contract

An adapter should provide:

- Capability observation tied to executable identity/version and relevant configuration.
- Supported entry modes and native session identity observation.
- Submission with message ID, attempt ID, recipient generation, and native correlation IDs.
- Native activity and completion observations with bounded uncertainty.
- Clear classification of unsupported operation, approval wait, startup failure, connection loss, and ambiguous delivery.
- Shutdown/reconnect behavior consistent with the core lifecycle contract.
- Bounded, sanitized diagnostics and a minimal conformance interface for testing.

The adapter must serialize or otherwise coordinate native submissions where a harness requires it. Concurrent messages cannot create overlapping turns that violate the harness contract.

Bounded waits must not exhaust the workers needed for send, acknowledgement, cancellation, or lifecycle operations. Apply documented concurrency limits and backpressure; a small fixed thread pool is not evidence of adequate capacity for many waiting clients.

### 7.3 Capability vocabulary

For each capability, record **supported**, **unsupported**, or **unverified**, its evidence source, observation time, and relevant version/configuration fingerprint. Distinguish capabilities such as:

- Tool access through MCP.
- Native delivery while idle.
- Submission while another operation is active.
- Model handling before the current turn ends.
- Managed launch, attachment, and resume.
- Native identity and tool-call observation.
- Safe retry or duplicate suppression.

Do not infer one capability from another. Version strings are useful provenance; schema probes and actual behavior provide stronger evidence. Invalidate observations after relevant updates.

Claude channel registration logs, for example, were used as a version-sensitive startup gate. Registration alone was explicitly not an ingestion acknowledgement. [P3], [P6]

### 7.4 MCP compatibility boundary

MCP standardizes access to tools and resources. Host applications determine how resource context enters their model interaction. Base resource notifications therefore cannot serve as a universal proof of session wake-up or model ingestion. [M1]

The inspected PoC implements MCP revision `2025-06-18`. Current published revision `2026-07-28` changes protocol sessions, discovery, and notification mechanisms and deprecates sampling. Xordination should support the protocol revisions actually required by its target clients, publish that matrix, and use a maintained protocol implementation where practical. It should not use sampling as an assumed cross-harness session messaging mechanism. [P7], [M2]

Xordination participant IDs, generations, idempotency keys, and durable delivery state are application-level concepts. Their correctness must survive MCP connection or protocol lifecycle changes.

## 8. Configuration, permissions, and message authority

**CFG-1.** Project/space endpoints, selected adapters, and owned integration files are explicit and inspectable. Support worktree-specific local paths without changing shared space identity.

**CFG-2.** Preserve native operator authentication, model choices, approval policies, hooks, settings, and existing MCP configuration. Apply only the integration changes needed for the selected scope; detect conflicts.

**CFG-3.** Proof-only restrictions must not enter the generic product by accident. Disabling built-in tools, allowing only proof tools, forbidding delegation, and enforcing fixture instructions were experiment controls. Any consumer-specific tool restriction belongs to that consumer. [P5], [P8]

**CFG-4.** Bind credentials and access decisions to the authorized project/space and participant. Enforce visibility for peer lists, messages, replies, status, and recovery operations.

**CFG-5.** For cross-machine deployment, provide authenticated connections and transport protection appropriate to the endpoint. Loopback is an initial deployment mode, not a universal security boundary.

**CFG-6.** A peer message is attributed peer content. Routing it does not elevate it above the operator's instructions or grant additional tool permissions. Preserve provenance in its native rendering.

**CFG-7.** Logs and exports exclude credentials and make content retention explicit. Verbose native transcripts should be selectable diagnostics with clear ownership and retention.

**CFG-8.** The service must not silently change a model, select a substitute harness, create a replacement session, or bypass an approval to make a readiness test pass.

**CFG-9.** Endpoint proxy behavior is scoped and documented. Protect local credentials from accidental proxy forwarding while retaining legitimate proxy routing for remote endpoints. Do not apply a global proxy bypass as a generic networking fix.

## 9. Xorder delivery specification

Xordination owns its implementation and release version. Xorder distributes verified releases using its existing resource model. [X1], [X2]

| Resource | Proposed content |
| --- | --- |
| Executable/package resource | Broker, MCP frontend, adapters, runtime management, runtime prerequisites |
| Optional OCI image | The same release in a reproducible container with a documented persistent-state mount |
| Configuration resource | Harness-specific project connection examples and owned installation destinations |
| Context resource | Compact communication instructions, delivery vocabulary, and recovery guidance |
| Profile | A pinned composition appropriate to the observed target environment |

Exact resource identifiers and image names should be chosen during packaging. The labels above are proposals, not existing catalog entries.

### 9.1 Installation and discovery

The Xorder URL remains sufficient for agent discovery. A usage page must explain:

1. Supported platforms, runtimes, harness versions, and measured deployment surfaces.
2. How to select an exact release identity and its compatible resources.
3. How to create or select the shared collaboration space.
4. How local processes or container clients reach the broker.
5. Where state, credentials, and owned configuration live.
6. How to join, inspect peers, exercise communication, stop, and recover.
7. Which capabilities were verified and which remain unsupported or unverified.

Native package installation should be available independently of Docker where the chosen runtime supports it. The optional sidecar image lets projects update communication independently of language toolchain images.

Do not install all vendor CLIs and credentials into every toolchain image merely to provide the broker. Adapter placement should follow the actual harness location and network boundary.

### 9.2 State and network topology

Local and container recipes must agree on endpoint identity and space. Container clients cannot assume the host's loopback address refers to the broker. Cross-sandbox deployment requires a reachable authenticated endpoint; availability must be checked in the actual environment.

Persist message state outside ephemeral containers. Recreating the broker container must not discard accepted messages. Keep per-harness adapter state and credentials appropriately scoped.

### 9.3 Readiness levels

| Level | Evidence |
| --- | --- |
| Package readiness | Correct release identity, executable/runtime prerequisites, usable state location |
| Service readiness | Broker responds, schema is usable, authorized connection works |
| Adapter readiness | Native session identity and required capability checks succeed |
| Communication readiness | A controlled message reaches the intended session and produces a correlated receipt/reply |

Expose these separately. A status report must say which levels passed.

Package validation can remain offline. Communication readiness may invoke paid/authenticated harnesses and should run when the operator requests live readiness or through an explicitly authorized verification workflow.

Xorder may verify a selected deployment using a small representative exchange. The full adapter compatibility suite belongs to Xordination's release process; it need not run on every unrelated Xorder change.

### 9.4 Updates and rollback

Pin executable hashes or image digests and compatible configuration/context revisions. Preserve established pins unless updating intentionally. Test store migrations and document rollback limits before an upgrade.

Xorder installation receipts establish delivered bytes and owned files. They do not prove a model followed instructions or that live communication succeeded. [X2], [X3]

## 10. Observability and operator experience

Default output should answer:

- Who is connected, and to which native session/generation?
- Which delivery capabilities are verified?
- Where is a particular message now?
- Did a peer acknowledge or reply?
- Is the system waiting for native approval, connection recovery, or explicit retry?
- What changed after a restart or replacement?

Correlate space, participant, generation, message, attempt, native session, native call/turn, and adapter version where available. Represent missing observations explicitly. Record requested model/reasoning separately from independently observed effective settings. The PoC recorded a Grok request of `grok-4.5`/`low` but observed `grok-4.6`/`xhigh`; Claude's effective effort was not independently established. A configured label is not effective-setting evidence. [P2]

Use one authoritative event ordering for broker state transitions. Record wire events under the same synchronization as writes so trace order matches actual output. Keep wall-clock timestamps for inspection and monotonic time for durations.

Provide bounded status summaries and cursor-based event reads. Detailed native traces should be opt-in or failure-triggered. Diagnostic bundles should carry source/build identity, effective configuration provenance, capability evidence, relevant event windows, and redaction status.

Distinguish failures of transport, native compatibility, approval, model compliance, and application work. A correct transport cannot guarantee a useful answer.

## 11. Verification and acceptance criteria

Tests should concentrate on observable contracts and previously demonstrated failure modes. Reuse a common adapter conformance suite with harness-specific fixtures. Keep model correctness, transport delivery, and consumer workflow tests separate.

### 11.1 Required acceptance matrix

| ID | Acceptance criterion | Primary verification |
| --- | --- | --- |
| AC-01 | Two sessions of the same harness are independently addressable | Broker contract test; representative live check |
| AC-02 | Repeating an identical send after a lost response returns one logical message; changed payload conflicts | Transactional integration test |
| AC-03 | Accepted messages survive broker restart; inbox reads do not remove them | Store/restart integration test |
| AC-04 | Unauthorized sender, space, reply, status, and acknowledgement operations are rejected | Broker authorization contract tests |
| AC-05 | Crash during ambiguous native submission remains uncertain and is not silently replayed | Fault-injected adapter/store integration test |
| AC-06 | Explicit retry keeps message identity, creates a new attempt, and rejects stale completions | Delivery lifecycle test |
| AC-07 | Replaced generations cannot send/claim/acknowledge or alter the new session under stale authority | Session race and recovery tests |
| AC-08 | Old telemetry and delayed native completion cannot be attributed to replacement sessions/new turns | Adapter/telemetry regression tests |
| AC-09 | Concurrent insertion and paginated inbox reads produce no skipped visible messages | Cursor contract test |
| AC-10 | Each claimed supported harness pair completes a native message/reply exchange with identity evidence | Opt-in live compatibility suite |
| AC-11 | Messages submitted during ongoing work preserve session identity and record actual handling timing | Opt-in live overlap scenario |
| AC-12 | Unsupported native methods and missing readiness evidence remain explicit | Adapter conformance failure cases |
| AC-13 | Installation preserves operator configuration and usable project tools | Configuration integration tests; focused live smoke |
| AC-14 | Selected native/package and container profiles retain state and reach the same broker | Packaging/deployment acceptance |
| AC-15 | Diagnostic event order follows actual wire order and correlation remains intact | Focused concurrency test |
| AC-16 | A project with no prescribed roles or work orders can use the service | Minimal independent consumer fixture |

For three fully supported harness types, test the six ordered cross-harness pairs as one parameterized scenario. Add a same-harness multi-session case. Avoid duplicating the same scenario across every consumer repository and image.

### 11.2 Preserve the PoC's evidence discipline

A live exchange should carry a fresh challenge and demonstrate that the recipient retains earlier context in the same native session. Correlate the native tool call or equivalent independent observation with the broker event and reply.

Report submission during busy work and handling during that work separately. A reply after the current turn ends can prove eventual delivery while failing a stronger in-turn claim. Missing timing evidence remains unknown.

Use the PoC's synthetic work fixture when useful as a known regression scenario; it is not a required workload or tool catalog for ordinary Xordination users. [P2], [P3], [P9]

### 11.3 CI and performance

- Run deterministic core and adapter conformance tests on relevant changes.
- Run packaging checks for affected artifacts.
- Keep live model-dependent verification explicit, bounded, and associated with release/compatibility evidence.
- Re-run live tests when native adapters or relevant harness versions change.
- Record source, executable versions, capabilities, prompts/fixture identity, and results for reproducibility.
- Measure idle CPU/memory, queue acceptance latency, native submission latency, end-to-end response latency, burst behavior, and recovery time separately.
- Establish initial baselines before selecting performance thresholds. Model latency must not hide broker regressions or be mistaken for broker cost.

A release claim should identify its tested matrix. Untested platforms or entry modes remain visible capability gaps.

### 11.4 Focused regression contracts

Exercise these behaviors with focused fixtures. They are product verification requirements, not claims that tests were executed for this specification.

| Scenario | Required contract |
| --- | --- |
| Concurrent discovery, delivery, acknowledgement, and restart | Operations remain usable concurrently; receipts and accepted messages survive restart |
| Transport loss during native submission | Preserve ambiguity and require explicit retry |
| Session replacement during delivery or recovery | A retired generation cannot claim or settle replacement work |
| Concurrent output and trace recording | Trace order matches wire order |
| Historical transcript ingestion | Old entries cannot be attributed to a replacement session |
| Initialization with partial operator configuration | Existing operator settings remain authoritative |
| Binding changes after an operation captures its context | Fence publication against the captured scope; do not publish into a replacement binding |

## 12. Reuse and extraction plan

| Element | Implementation direction | Required boundary |
| --- | --- | --- |
| PoC native adapter mechanics | Preserve interface knowledge and evidence correlation | Remove fixture orchestration, fixed three-peer assumptions, and proof-only tool policy |
| PoC compatibility probes | Adapt into capability observations | Version/configuration provenance, invalidation, stable external result shape |
| PoC verifier and scenarios | Retain as compatibility regression assets | Separate test controller from production runtime |
| Durable mailbox | Implement acceptance, attempts, receipts, uncertainty, and explicit retry | Project/space scope and optional consumer metadata |
| Session generation checks | Enforce lifecycle invariants | Keep role binding and assignment-specific recovery policy outside the core |
| Telemetry | Enforce ordering and attribution safeguards | Use generic identity/correlation fields and bounded diagnostics |
| Native configuration handling | Preserve operator ownership | Avoid fixed role/model/catalog assumptions |
| Xorder resources and profiles | Add verified release definitions and usage | Keep Xordination independently versioned and installable |

Inspect dependencies before copying modules. Keep communication independent of role management, editor tooling, repository workflows, and worktree management.

Extraction may use the existing language first or reimplement a defined contract. Both paths should retain the same behavioral acceptance contracts and public PoC compatibility evidence. A rewrite must not reset the evidence baseline.

## 13. Suggested implementation sequence

This sequence is a proposal, not a change to the overall product goal.

1. **Define and implement the core contract:** identity, durable send/reply/inbox/ack, attempts, uncertainty, retry, cursors, and bounded status. Validate using controlled clients and injected failures.
2. **Extract native adapters:** preserve persistent sessions and independently observed identity; remove proof and consumer policy coupling.
3. **Exercise lifecycle and live communication:** cross-harness pairs, multiple same-harness sessions, restart, replacement, approval waits, and work overlap.
4. **Verify session entry and deployment modes:** investigate attachment/resume per harness and local/container/remote reachability; record exact capability gaps and supporting evidence.
5. **Publish through Xorder:** package/image, configuration/context, profile, exact release evidence, and reproducible readiness recipe.
6. **Integrate consumers:** prove independent project adoption, then integrate coordinators through the same public contract and their own policy layers.

Each stage produces reviewable evidence. Passing an early stage does not establish full release readiness.

## 14. Decisions still open

| Decision | Current recommendation | Information needed |
| --- | --- | --- |
| Implementation language | Selected for now by owner direction: Python 3.10+ with the standard library only at runtime, matching the initial implementation | Revisit only on owner direction; a different language would retain the same behavioral contracts and evidence baseline |
| Runtime packaging | Native package plus optional sidecar image | Required OS/architecture matrix and runtime dependencies |
| Durable backend | Single-broker SQLite first | Measured concurrency, operational requirements, migration approach |
| Existing-session integration | Preserve attachment/resume as explicit adapter capabilities | Current native APIs and live evidence per harness/version |
| Remote exposure | Support a reachable authenticated broker deployment | Actual cloud/container ingress, egress, endpoint ownership, and auth integration |
| Offline routing and handover | Queue with explicit destination policy; require explicit transfer across generations | Preferred operator experience for departed/replaced peers |
| Broadcast/channels | Keep delivery per recipient; add group addressing if needed | Concrete consumer use cases |
| Retention and limits | Bounded, discoverable defaults | Initial usage measurements and diagnostic requirements |
| Public names and paths | Keep a small `xordination_*` tool surface | Final schema and packaging review |

These decisions should be resolved with targeted evidence or owner direction. Missing verification must not be converted into a claim that a capability is impossible.

## 15. Source references

All repository links below are public and pinned to the inspected revisions.

### PoC

- **P1 — README and architecture overview:** https://github.com/xormania/multi-harness-proof/blob/ad294398501ed119a436db54eca784be2fb10e76/README.md
- **P2 — Historical live results and tested settings:** https://github.com/xormania/multi-harness-proof/blob/ad294398501ed119a436db54eca784be2fb10e76/LIVE_RESULTS.md
- **P3 — Protocol, evidence, startup, and measured limits:** https://github.com/xormania/multi-harness-proof/blob/ad294398501ed119a436db54eca784be2fb10e76/REFERENCE.md
- **P4 — Relay, identity, in-memory queues, and loopback client:** https://github.com/xormania/multi-harness-proof/blob/ad294398501ed119a436db54eca784be2fb10e76/mhproof/relay.py
- **P5 — Native adapters:** https://github.com/xormania/multi-harness-proof/blob/ad294398501ed119a436db54eca784be2fb10e76/mhproof/adapters.py
- **P6 — Installed capability and Claude registration probes:** https://github.com/xormania/multi-harness-proof/blob/ad294398501ed119a436db54eca784be2fb10e76/mhproof/compat.py
- **P7 — MCP frontend and protocol revision:** https://github.com/xormania/multi-harness-proof/blob/ad294398501ed119a436db54eca784be2fb10e76/mhproof/mcp.py
- **P8 — Proof-specific model contract and hardcoded peers:** https://github.com/xormania/multi-harness-proof/blob/ad294398501ed119a436db54eca784be2fb10e76/mhproof/contract.py
- **P9 — Testing guidance:** https://github.com/xormania/multi-harness-proof/blob/ad294398501ed119a436db54eca784be2fb10e76/TESTING.md

### Xorder

- **X1 — Resource discovery and current resource kinds:** https://github.com/xormania/xorder/blob/3219d1c41dd7b60f70542b02865d9ce2748dd348/README.md
- **X2 — Architecture, resource boundaries, exact identity, and application:** https://github.com/xormania/xorder/blob/3219d1c41dd7b60f70542b02865d9ce2748dd348/docs/architecture.md
- **X3 — Profiles, target assignments, and installation receipts:** https://github.com/xormania/xorder/blob/3219d1c41dd7b60f70542b02865d9ce2748dd348/docs/profiles.md

### Protocol context

- **M1 — MCP resources and host-controlled context inclusion, revision 2026-07-28:** https://modelcontextprotocol.io/specification/2026-07-28/server/resources
- **M2 — MCP revision 2026-07-28 changes and deprecations:** https://modelcontextprotocol.io/specification/2026-07-28/changelog

The protocol references establish the MCP boundary. They do not establish current vendor adapter support; that requires installed-version checks and live evidence.


[P1]: https://github.com/xormania/multi-harness-proof/blob/ad294398501ed119a436db54eca784be2fb10e76/README.md
[P2]: https://github.com/xormania/multi-harness-proof/blob/ad294398501ed119a436db54eca784be2fb10e76/LIVE_RESULTS.md
[P3]: https://github.com/xormania/multi-harness-proof/blob/ad294398501ed119a436db54eca784be2fb10e76/REFERENCE.md
[P4]: https://github.com/xormania/multi-harness-proof/blob/ad294398501ed119a436db54eca784be2fb10e76/mhproof/relay.py
[P5]: https://github.com/xormania/multi-harness-proof/blob/ad294398501ed119a436db54eca784be2fb10e76/mhproof/adapters.py
[P6]: https://github.com/xormania/multi-harness-proof/blob/ad294398501ed119a436db54eca784be2fb10e76/mhproof/compat.py
[P7]: https://github.com/xormania/multi-harness-proof/blob/ad294398501ed119a436db54eca784be2fb10e76/mhproof/mcp.py
[P8]: https://github.com/xormania/multi-harness-proof/blob/ad294398501ed119a436db54eca784be2fb10e76/mhproof/contract.py
[P9]: https://github.com/xormania/multi-harness-proof/blob/ad294398501ed119a436db54eca784be2fb10e76/TESTING.md
[X1]: https://github.com/xormania/xorder/blob/3219d1c41dd7b60f70542b02865d9ce2748dd348/README.md
[X2]: https://github.com/xormania/xorder/blob/3219d1c41dd7b60f70542b02865d9ce2748dd348/docs/architecture.md
[X3]: https://github.com/xormania/xorder/blob/3219d1c41dd7b60f70542b02865d9ce2748dd348/docs/profiles.md
[M1]: https://modelcontextprotocol.io/specification/2026-07-28/server/resources
[M2]: https://modelcontextprotocol.io/specification/2026-07-28/changelog
