# Compatibility and readiness evidence

Xordination 0.1.0 contains native adapter mechanics informed by the inspected public PoC. No authenticated live harness run for Xordination is recorded by this initial change. Do not advertise a harness version as certified from the PoC's historical result or a version string alone.

## Delivery capability matrix

| Client/harness | Integration path | Managed launch | Existing-session attachment / resume | Current live communication evidence |
| --- | --- | --- | --- | --- |
| External MCP client | Stdio tools over the authenticated shared HTTP broker | Client-managed | Client-managed; no native identity inferred | Deterministic client/MCP tests establish mailbox access only |
| Codex | Persistent app-server session; `turn/start.toolOutput` and dynamic communication tools | Adapter path provided; installed `TurnStartParams.toolOutput` and `ThreadStartParams.dynamicTools` schemas must allow required methods | Unverified product target; explicit mode refuses fallback | Unverified for Xordination |
| Claude Code | Channel-enabled stdio integration; SessionStart hook observes native identity and fresh debug-log registration gates channel readiness | Adapter path provided; channel approval/configuration may be required | Unverified product target; explicit mode refuses fallback | Unverified for Xordination |
| Grok Build | ACP session prompt/interjection and native session observations | Adapter path provided; required ACP/extension behavior must be available | Unverified product target | Unverified for Xordination |

The installed `doctor` result is a capability observation, not native ingestion evidence. A missing executable, unsupported schema, approval wait, unobserved native identity, or incomplete exchange must be explicit. Never infer model handling during a turn from successful submission while a tool is outstanding.

## Evidence record for a verified combination

Record:

- Xordination source commit, package/image identity, Python/OS identity.
- Harness executable location, observed version, relevant configuration fingerprint, and capability probe output.
- Requested and independently observed native model/reasoning settings separately.
- Space, participants, generations, independently observed native sessions, and correlation IDs.
- Fresh challenge, retained earlier session context, message/attempt ID, native observation, authenticated receipt and/or correlated reply.
- Submission time, outstanding work/native turn state, actual handling time when observed, completion and uncertainty.
- Results for each ordered pair independently, including incomplete results and sanitized failure evidence.

For three supported harness types, use one parameterized scenario for the six cross-harness ordered pairs plus a same-harness multi-session scenario. Include a representative ongoing-work exchange without assuming simultaneous model computation. Paid/authenticated tests are opt-in; ordinary CI uses controlled protocol fixtures.

## Readiness levels

| Level | Passing evidence | Insufficient evidence |
| --- | --- | --- |
| Package | Installed correct version, console entry point on `PATH` attributable to that installation, writable durable-state location | Checkout exists |
| Service | Shared broker state and authenticated participant operation work | Listening port alone |
| Adapter | Required installed native methods and session identity observations succeed | Executable/version label alone |
| Communication | Intended native session handles a fresh exchange with correlated receipt/reply evidence | Queue insertion, MCP handshake, or channel registration alone |

A report returns each level separately. Communication readiness does not establish application-work completion or useful model reasoning.

`xordination readiness` produces this report without starting native sessions; see the [README](../README.md#readiness-report). Its adapter level combines the installed `doctor` capability with native session identity observed in supplied passing evidence. Its communication level is `unverified` unless a retained operator-native verification report is supplied whose source, configuration and participant executable/version fingerprints match the current package and `doctor` observations.

## Protocol and lifecycle limits

The initial stdio frontend negotiates **2025-06-18** and **2025-11-25**. The first is the revision used by the inspected PoC. Other protocol revision support requires explicit negotiation/conformance work; application-level space/participant/generation/attempt correctness must survive MCP reconnects and upgrades.

Managed launch, adapter reconnect, broker restart, native resume, and native replacement are separate events. Replacing a participant creates a new binding generation and fences old authority. Old-generation messages remain identifiable; their transfer requires an explicit recovery policy. Broker restart preserves accepted records and must keep ambiguous prior claims uncertain.

The broker does not recover lost native reasoning context or transparently migrate a conversation between vendors. Shutdown must only stop adapter-owned resources; externally joined sessions remain operator-owned.

## Deployment verification

Local package and sidecar recipes are included. Cross-machine and cross-sandbox use require a reachable authenticated protected endpoint in the actual environment. A client's loopback address is local to that client. Native vendor credentials do not belong in the broker image. Protect local participant credentials from accidental proxy forwarding; scope any remote proxy configuration to the intended broker endpoint.

POSIX state files are restricted to their owner before SQLite opens them. Existing state-directory permissions remain operator-owned; newly created state directories are private. Windows deployment requires appropriate OS access controls for the database, sidecars, and token-bearing descriptors.

## Historical evidence

[Specification §2](specification.md#2-evidence-and-source-baseline) pins the inspected sources. The PoC records a live run on 2026-09-20 with 15/15 message checks, 9/9 work batches, and 69/69 matched native tool observations. Those results establish the earlier implementation's tested paths under its recorded limits. Xordination needs its own versioned results before making the same claims.

