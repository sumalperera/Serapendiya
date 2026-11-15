# Apache Iggy reference review

Status: research input, 2026-11-15. This review changes no gate, storage
contract, dependency or accepted ADR. PR #34 merged the ADR-024 journal
material as a review artifact; it did not accept a format or recovery protocol.
G1 acceptance remains open; G2 and later require their own authorization.

## Reference and scope

Inspected Apache Iggy's server release [`server-0.10.0`](https://github.com/apache/iggy/releases/tag/server-0.10.0), commit
[`91e0206367aad2d30259f7d45203832b08ae84e7`](https://github.com/apache/iggy/tree/91e0206367aad2d30259f7d45203832b08ae84e7),
and the linked upstream documentation on 2026-11-15. Iggy is a relevant Rust
streaming system, but its cluster, failure model and defaults are not assumed
to match ours. Its implementation and documentation are evidence of its design,
not proof of Serapendiya's behavior or performance. No upstream benchmark was
independently reproduced for this review.

## Findings and follow-up questions

| Area | What the reference shows | Serapendiya planning and evidence |
| --- | --- | --- |
| Durability | Iggy's [durability documentation](https://iggy.apache.org/docs/server/durability/) distinguishes message and consumer-offset durability, which default independently to `replicated`. On a single node this default need not sync to stable storage before success. `persisted` requires a recoverable copy on stable storage at the replication quorum; on one node that requires local persistence. Setting message durability alone does not change offset durability. | Keep sync-before-ACK and certified-range refusal. Make durability and consumer progress understandable separately. Compare performance only after matching durability, replication topology, payloads and workload; validate the actual configuration and outcome. |
| Journal authority | Iggy separates its metadata WAL from partition durability. Its [partition journal](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/journal/src/partition_journal.rs) describes two in-place frontier slots and a version change to prevent old readers misinterpreting them; a separate [superblock implementation](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/journal/src/superblock.rs) alternates two files. | Include dual-slot frontiers in the ADR-024 alternatives review: define slot writes, torn-write assumptions, equal or damaged slots, directory durability, old-reader refusal and the acknowledged boundary. Iggy's selection rule applies to its own protocol; it does not authorize promoting `manifest.tmp`, prove rollback detection, or accept a Serapendiya format. Compare with the single-manifest and J1 candidates under matched conditions. |
| Runtime and memory | Iggy's [architecture](https://iggy.apache.org/docs/introduction/architecture/) describes partition ownership, bounded dispatch and platform-specific I/O. | Consider ownership and bounded admission for ADR-005. Measure the full memory budget, queue saturation and portability; a bounded pool alone does not establish a global memory bound. Do not infer that its runtime is faster for our workload. |
| Kafka | Iggy documents a separate [Kafka gateway](https://iggy.apache.org/docs/gateways/kafka/); its pinned [scope](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/gateways/kafka/docs/SCOPE.md) defines supported operations and limitations. | At the authorized wire-protocol gate, compare a gateway with native handling, including supported versions, explicit errors, real-client tests, authorization, unknown outcomes and the cost of an extra hop. A gateway does not establish Kafka compatibility or change the current scope. |
| Iceberg | The pinned [Iceberg sink README](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/connectors/sinks/iceberg_sink/README.md) warns that routing may skip records, multi-table commits are not atomic, and failure/replay interactions can lose or duplicate records. These statements concern that sink and configuration, not all Iggy connectors. | For any future sink, specify source-offset advancement, durable sink status, retention pins, retries and reconciliation before implementation. Test invalid routes, partial fan-out, failures around commit and offset advancement, restarts and duplicate delivery. Do not claim exactly-once from an Iceberg commit alone. |
| Crash evidence | Iggy's pinned [crash tests](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/core/integration/tests/cluster/crash_durability.rs) distinguish process kills from actual power loss. | Keep deterministic fault injection, process-crash tests and separately reported block/VM power-loss replay as distinct evidence. Tests for an existing format do not certify a new journal. |
| Operator tools | Iggy's [MCP documentation](https://iggy.apache.org/docs/ai/mcp/) illustrates exposing streaming operations to AI tooling. | Use it as a later usability reference. Preserve the project's read-only-first and authorization gates; exposing a tool does not make mutation or data access safe by default. |

Iggy's architecture describes a 1 GiB soft segment target. It is not a workload
measurement or a decision to replace the 128 MiB planning target in ADR-024.
Segment overshoot, journal rotation, full-scan startup, disk use and manifest
cost need matched measurements under the selected protocol.

## Relation to existing references

The founder's standing comparison still applies. Reuse the earlier pinned
[segment-cap alternatives review](../adr/ADR-024-segment-cap-alternatives-review.md)
and revalidate relevant details at implementation time:

- **Apache Kafka:** segmented logs and client protocol behavior are relevant;
  replication and acknowledgment settings must match before comparing durability.
- **Redpanda:** segment lifecycle and Kafka-compatible operations are relevant;
  its architecture and defaults do not establish our storage guarantees.
- **ReductStore:** storage and retention trade-offs are relevant; its workload
  and API are not assumed to match a partitioned broker.
- **Liftbridge:** segmented-log and recovery approaches are relevant historical
  context; check maintenance and applicability before considering reuse.
- **Apache Iggy:** partition durability, tooling and connector boundaries add a
  Rust streaming reference; its metadata WAL and partition journal have distinct
  purposes and should not be conflated with one another or with J1.

## Reuse and validation boundary

Iggy is Apache-2.0 licensed. This review imports no code or dependency and does
not change Serapendiya's MIT license. Any future reuse needs the project's
[provenance review](../../PROVENANCE.md), applicable notices, maintenance,
platform and dependency checks. Its pinned
[toolchain](https://github.com/apache/iggy/blob/91e0206367aad2d30259f7d45203832b08ae84e7/rust-toolchain.toml)
is newer than Serapendiya's current Rust 1.85 baseline; that file alone does not
establish Iggy's minimum supported Rust version or integration compatibility.

Before selecting any mechanism, write the exact failure model and acceptance
criteria, review alternatives, build independent fixtures and failure tests,
and measure the candidate against a matched baseline. These investigations
remain subject to the existing ADR and later-gate approvals.
