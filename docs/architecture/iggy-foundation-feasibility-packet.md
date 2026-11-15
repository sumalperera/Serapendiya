# Iggy foundation feasibility packet

Status: **proposed; not an architecture decision or implementation authority**.
Inspection date: 2026-11-15. This packet evaluates a foundation for a generic,
industry-adaptable messaging and integration platform. It does not accept an
Iggy fork, a Serapendiya format, a durability relaxation, a migration or a
later delivery gate. The product's private commercial plans and data remain
outside this document.

## Problem, scope and present evidence

The current [roadmap](../roadmap.md) describes an experimental local storage
implementation: contiguous offsets, checksummed frames, a certified checkpoint,
recovery and fault tests. It is not a running broker; G1 acceptance is open.
The user-facing objective requires reliable source ingestion, mapping and
enrichment, orchestration, destination delivery, inspection and controlled
replay, while allowing different industry and customer adapters. Broker work
must be assessed against time to demonstrate that workflow and continuing
maintenance, without assuming a broker provides the workflow by itself.

We inspected selected Apache Iggy `server-0.10.0` release files at commit
[`91e0206367aad2d30259f7d45203832b08ae84e7`](https://github.com/apache/iggy/tree/91e0206367aad2d30259f7d45203832b08ae84e7)
and upstream documentation. This is targeted source and document review, not
an exhaustive code audit, independent test, benchmark or approval. The linked
upstream web documentation can change; revalidate release behavior and settings
against the pinned source when running experiments. No upstream code is copied.
The existing [Iggy reference review](iggy-reference-review-2026-11-15.md) and
[ADR-024 alternatives](../adr/ADR-024-segment-cap-alternatives-review.md)
provide prior context. PR #34 merged research; its proposed formats and
protocols were not accepted.

## Requirements-to-code map

| Contract or capability | Pinned upstream evidence | Assessment and unresolved questions |
| --- | --- | --- |
| Successful durable message | Iggy's [durability documentation](https://iggy.apache.org/docs/server/durability/) says singleton `persisted` completion follows log/index writes, file synchronization and synchronization of newly created segment names. The independent `replicated` default can acknowledge data still in process memory. [Partition persistence](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/persistence.rs) implements persistence work. | Useful starting point. Trace the actual selected singleton path and compare its authority, barrier and error handling with the accepted local contract. Do not assume matching syscall names establish equivalent crash or corruption semantics. |
| Certified corruption and recovery | [Segment recovery](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/segment_recovery.rs) contains index/log recovery and refusal logic. [Partition WAL](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/journal/src/partition_journal.rs) describes a two-slot in-place frontier and old-format refusal. A separate [superblock](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/journal/src/superblock.rs) stores VSR state using alternating files. | Identify which authority actually protects each acknowledged singleton and replicated message; do not conflate the partition WAL, VSR superblock and message segment. Need tests for missing acknowledged tail, damaged certified data, interrupted roll, unknown outcome and old-reader behavior. External coherent rollback remains a distinct problem. |
| Offset persistence | [Offset storage](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/partitions/src/offset_storage.rs) separates file sync and directory sync; [upstream documentation](https://iggy.apache.org/docs/server/durability/) says explicit offset durability is independent of message durability and poll auto-commit is asynchronous. | Configure and verify messages and offsets independently. Trace completion, failure and retention behavior; a successful poll is not proof that an offset update completed. |
| Source delivery | The source runtime's [producer state mechanism](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/connectors/runtime/src/exactly_once.rs) describes a producer ID, epoch and batch sequence coupled to plugin state; [source runtime](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/connectors/runtime/src/source.rs) uses it in a configured mode. | Investigate its prerequisites and limits rather than rebuilding it by default. Its documented source-to-partition behavior does not establish exactly-once downstream side effects, arbitrary-provider replay or cross-partition transactions. |
| Sink delivery | [Connector documentation](https://iggy.apache.org/docs/connectors/introduction/) describes sink offsets advancing on poll before external processing completes; [sink runtime](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/connectors/runtime/src/sink.rs) exposes plugin consumption. | For a chosen pinned runtime/configuration reproduce the sequence and failure case. Compare changing the runtime, a separately controlled consumer, and another maintained framework. Require a durable checkpoint only after the destination outcome is known or reconciled; define unknown results and duplicate handling. |
| Retention and identity | [Segment cleaner](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/server/src/segment_cleaner.rs), [topic options](https://iggy.apache.org/docs/server/topic-options/) and [concepts](https://iggy.apache.org/docs/introduction/concepts/) describe retention, stored-offset barriers and reusable numeric IDs. | Test whether persisted offsets can support each needed queue, mapping, replay and sink retention pin. With no stored offset the documented barrier is absent. Define stable source incarnation/identity and explicit retention exhaustion behavior; numeric IDs alone are not historical identity. |
| Kafka | Pinned [gateway scope](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/gateways/kafka/docs/SCOPE.md) lists API versions and limitations; [gateway guide](https://iggy.apache.org/docs/gateways/kafka/) documents a separate service and disabled-by-default bridge. | Verify the actual pinned gateway/client/configuration. Advertise only a tested JVM/librdkafka matrix. Include bridge failure, unsupported APIs and persistence settings; no general compatibility claim. |
| Mapping, enrichment, orchestration | Iggy's [connector SDK](https://iggy.apache.org/docs/connectors/sdk/) supplies source and sink extension points. | A product-specific mapping lifecycle, approvals, effective versions, event semantics, routing and cross-system evidence are not established by generic connectors. Decide external service versus fork modifications after the synthetic slice. |
| Queue and replay | Existing Iggy consumer groups and offsets are described in its [concepts](https://iggy.apache.org/docs/introduction/concepts/). | Partition assignment is not proof of per-message lease fencing, durable ACK, retry or DLQ. Bounded replay preview must use independent state and have no live side effects. Define and test before claiming these features. |

## Candidate foundations

| Candidate | Potential benefit | Cost and specific unresolved risk |
| --- | --- | --- |
| A. Continue current Serapendiya engine | Control over certified-boundary protocol and existing tests. | Must build and maintain broader server, protocol, lifecycle and operations; storage acceptance remains open. |
| B. Use Iggy via public interfaces | Fastest way to investigate fit while benefiting from upstream updates; less fork ownership. | APIs, connector behavior and failure guarantees may not meet requirements; test before adoption. |
| C. Maintain an Iggy fork | Can alter storage, delivery, API or operational behavior where justified. | Own patch divergence, upgrades, source/license obligations and correctness of changes across storage and replication. |
| D. Other maintained engine or component | May fit a specific protocol, state or delivery requirement better. | Adds selection and integration work; revisit current primary-source evidence and supported versions. |
| E. Hybrid Iggy transport plus separate state/evidence store | Could preserve generic transport while managing mapping versions, checkpoint and scoped deduplication state separately. Existing Serapendiya storage is one unaccepted candidate; maintained alternatives must also be evaluated. | Two durable systems do not become one transaction. Define the authoritative payload, binding to identity/offset, publish and checkpoint order, unknown results, duplicate effects, retention, backup and cross-store recovery. A separate checkpoint cannot prove missing Iggy bytes were retained. |

Working preference: investigate B before internal fork changes; contribute
generic changes upstream when practical. This is a research priority, not an
accepted selection or a prohibition on C. Record a specific requirement, failed
or insufficient alternatives, patch owner, tests and upgrade cost before
proposing internal changes. Estimate effort only after the baseline and spike;
no comparative performance or schedule is established here.

## Existing-system comparison

The prior [segment-cap review](../adr/ADR-024-segment-cap-alternatives-review.md)
and [Iggy review](iggy-reference-review-2026-11-15.md) record upstream revisions
and inspection dates. This packet reuses that research; it is not a new
line-by-line survey of every project. Revalidate the exact mechanism and
license before adoption.

| Reference | Prior pinned primary source | Relevance and deliberate distinction |
| --- | --- | --- |
| Apache Kafka | [LogSegment at `0e67542`](https://github.com/apache/kafka/blob/0e675429a2d15605322ac6a8e3077433cbe15732/storage/src/main/java/org/apache/kafka/storage/internals/log/LogSegment.java) | Segment lifecycle and Kafka interoperability are relevant; replication/ACK settings are not assumed equivalent to a local stable-storage contract. |
| Redpanda | [segment appender at `4254992`](https://github.com/redpanda-data/redpanda/blob/4254992f6115e92c1dbd86f90bb0aee0b869f15e/src/v/storage/segment_appender.cc) | Relevant segmentation and append reference. Its implementation and defaults are not evidence of Serapendiya correctness; check licensing before any reuse. |
| ReductStore | [block manager at `8cb5009`](https://github.com/reductstore/reductstore/blob/8cb50097dd09c5a89dcc1d1f8508ace6c2bbb4d0/reductstore/src/storage/block_manager.rs) | Relevant storage lifecycle comparison; different workload and API make direct protocol equivalence unproven. |
| Liftbridge | [segment at `af2a662`](https://github.com/liftbridge-io/liftbridge/blob/af2a662695ce67c099e56d24444afb15bd8df232/server/commitlog/segment.go) | Historical segmented-log comparison; verify maintenance and relevance before choosing any component. |
| Iggy | Pinned release and code above. | Strong Rust broker starting point; the tests and cross-system contracts below remain ours to prove. |

## Proposed validation before implementation

All tests below are **planned, not run in this packet**. An isolated baseline
can be scoped separately; persistent format changes, production deployment and
later-gate implementation require their existing authorizations. Set numeric
targets with the founder before measurement; no invented latency or schedule
threshold is implied.

1. **Baseline:** build the exact pinned Iggy release in an isolated environment;
   record source SHA, toolchain, OS/filesystem, tests and config. Exercise
   awaited native append, explicit offset store, read and restart with message
   and offset policies independently configured. Identify the actual execution
   path and avoid assuming a `202` dispatch response proves persistence.
2. **Durability comparison:** trace append, sync, publication and recovery for
   current Serapendiya, proposed ADR-024 and pinned Iggy. Enumerate failure
   cuts before/after writes, each file/directory sync, rollover and reply;
   check missing acknowledged tail, corruption, sync failures and ambiguous
   outcomes. Report observed refusal or open behavior and preserve evidence.
3. **Crash evidence:** separately run deterministic injected faults,
   process-kill tests and, when authorized and feasible, ext4 and XFS block/VM
   replay. Record cut points, samples and platform; a passing process-kill test
   is not a power-loss test, and a finite zero-failure sample is not a proof.
4. **Connector behavior:** reproduce source producer-state prerequisites and
   sink checkpoint timing with a synthetic source and failing destination.
   Test lost responses, retries, crash between destination commit and checkpoint,
   duplicate messages and partial failure. Compare manual offset control and
   runtime changes; do not infer an end-to-end guarantee from a source feature.
5. **Synthetic industry-neutral slice:** two fake sources emit differently
   identified versions of one entity and a parameterized attribute. Preserve
   raw input, perform reviewed versioned mapping, publish to a test destination
   and trace source-to-destination progress. Exercise ambiguity, late updates,
   mapping corrections, source recovery snapshots, customer-specific routing,
   restart and duplicate delivery. No real provider data or live effects.
6. **Hybrid matrix:** if candidate E remains in contention, test every ordering
   between source receipt, evidence-store commit, Iggy append, mapping update,
   destination effect and checkpoint. Include restart from inconsistent backup
   points, missing message bytes, dedup expiry and downstream unknown outcome.
7. **Performance:** compare equivalent payloads, durability, replication,
   hardware, filesystem, concurrency, batching, warm-up and duration. Capture
   throughput, p50/p95/p99, sample counts, CPU/RSS, disk/syncs, end-to-end
   stage delays, recovery time, workload and raw results. Benchmark results for
   one contract do not establish another contract's latency.
8. **Client and operations:** check authorization, redaction and tenant
   separation, upgrade/refusal and rollback, and real Kafka clients for any
   selected compatibility scope. Provide an operator inspection or an explicit
   accepted unavailability outcome for each new refusal class.

In parallel, conduct customer discovery and review provider terms for access,
retention, redistribution, derived use and applicable prior confidentiality
obligations. Record unknown rights rather than implying authorization. No real
provider or customer connection is authorized by this document.

## Licensing, delivery and decision gates

Iggy is Apache-2.0 licensed; review its notices, source provenance, trademarks,
transitive dependencies, supported platforms and toolchain before importing or
distributing any fork. Serapendiya's existing MIT license does not erase
upstream license obligations. Any source import, packaging, format or migration
decision requires a concrete reviewed proposal and applicable approval.

The next decision packet should contain the tested behavior matrix, open gaps,
candidate maintenance estimates, provider-rights status and an explicit
recommendation. A merged review artifact is not format acceptance. Before a
release, record supported environments, failure assumptions, operator paths,
security review and independently reviewed evidence; do not claim that CI,
fixtures, a synthetic slice or vendor benchmarks alone prove crash safety.
