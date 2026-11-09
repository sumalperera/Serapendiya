# Serapendiya delivery roadmap

## Current stage: ideation plus technical foundation

Serapendiya is not presented as production-ready software. The project has a
working Rust foundation and an approved local-storage implementation, while the
customer-facing streaming workflow is still ahead.

### Open-source work to date

- G0: workspace, configuration, domain primitives, CLI skeleton, CI, ADRs, and
  provenance policy.
- G1 prototype: local append storage, CRC validation, checkpoints, recovery,
  derived indexes, bounded group commit, fault injection, crash harness, and
  Docker/Linux tests. G1 acceptance and crash-safety guarantees remain under review.

### Next authorized decision

The founder has selected a Redpanda-inspired direction: Kafka-compatible
behavior at the client boundary and an original, versioned Serapendiya storage
container. The concrete format and migration are proposed in
[the ADR-003 amendment](adr/ADR-003-hybrid-storage-proposal.md); an experimental
implementation is in progress and must be reviewed with the
[ADR-004 initialization proposal](adr/ADR-004-initialization-proposal.md).
PR #12's G1 safety fixes have been merged. G1 acceptance is still open.
The competing checksum and versioning options are assessed in the
[storage recovery review](adr/ADR-003-checksum-and-recovery-review.md).
The [ADR-005 proposal](adr/ADR-005-shard-ownership-proposal.md) records future
shard ownership, backpressure and benchmarking questions without authorizing
partition actors, a runtime change or replication.
The [agent trust-model proposal](adr/ADR-023-agent-trust-model-proposal.md)
records future identity, authorization and uncertain-publish requirements;
it does not authorize agent-facing features in G1.

G2 Kafka compatibility requires explicit authorization. It must remain a narrow,
tested subset with honest unsupported errors and real client evidence. No Kafka
wire implementation exists in this repository yet.

### Future product gates

- G3: independent broadcast and queue subscription state.
- G4: local developer workflow, diagnostics, and safe replay preview.
- G5: fuzzing, stress, security, recovery playbooks, and release hardening.

These stages are sequencing gates, not delivery-date promises.

## Planned end-to-end workflow

A future development milestone is:

```text
publish event → inspect event → restart process → recover event
```

That workflow should be built and tested as a vertical slice only after the
relevant gates are authorized. It must be presented as a prototype until client
compatibility, recovery, security, and operational claims are independently
verified.
