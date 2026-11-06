# Serapendiya delivery roadmap

## Current stage: ideation plus technical foundation

Serapendiya is not presented as production-ready software. The project has a
working Rust foundation and an approved local-storage implementation, while the
customer-facing streaming workflow is still ahead.

### Completed open-source gates

- G0: workspace, configuration, domain primitives, CLI skeleton, CI, ADRs, and
  provenance policy.
- G1: local append storage, CRC validation, durable checkpoints, recovery,
  derived indexes, bounded group commit, fault injection, crash harness, and
  Docker/Linux verification.

### Next authorized decision

G2 Kafka compatibility requires explicit authorization. It must remain a narrow,
tested subset with honest unsupported errors and real client evidence. No Kafka
wire implementation exists in this repository yet.

### Future product gates

- G3: independent broadcast and queue subscription state.
- G4: local developer workflow, diagnostics, and safe replay preview.
- G5: fuzzing, stress, security, recovery playbooks, and release hardening.

These stages are sequencing gates, not delivery-date promises.

## Investor-demo hypothesis

The smallest compelling demonstration is:

```text
publish event → inspect event → restart process → recover event
```

That workflow should be built and tested as a vertical slice only after the
relevant gates are authorized. It must be presented as a prototype until client
compatibility, recovery, security, and operational claims are independently
verified.

## Commercial separation

Paid or proprietary features belong in:

`https://github.com/sumalperera/Serapendiya-commercial`

The public repository must not contain commercial implementation code,
proprietary assets, entitlement checks, paid packaging, or private credentials.
Any shared open-source component must be independently useful, MIT-compatible,
and explicitly classified as open source before implementation.
