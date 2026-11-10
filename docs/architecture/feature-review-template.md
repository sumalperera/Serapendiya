# Feature engineering evaluation template

Use this structure in the applicable ADR or a short feature review before a
feature implementation or material redesign. Scale detail to the decision;
link existing evidence where it remains applicable. This is a decision record,
not an additional implementation authorization.

## Problem and existing behavior

- User outcome, implementation scope and applicable gate/ADR.
- Current mechanism and evidence of a correctness issue, avoidable cost or gap.
- Contracts to preserve: durability, ordering, identity, compatibility and bounds.

## Reference evaluation

| Reference | Version/tag/commit and primary source | Relevant mechanism or reason not applicable | Fit and trade-offs |
| --- | --- | --- | --- |
| Apache Kafka | | | |
| Redpanda | | | |
| ReductStore | | | |
| Liftbridge | | | |
| Other system, specification or reusable component, if relevant | | | |

Record the inspection date. Identify which paths were inspected; do not imply
an exhaustive upstream audit. Distinguish design study from source reuse.

## Selection and deliberate differences

- Compare the existing implementation, relevant alternatives and library reuse.
- Select an approach and explain its fit to the workload and supported platforms.
- Describe rejected alternatives and any proposed improvement over known designs.
- Record memory/resource bounds, operational and maintenance costs, security
  implications and required license/provenance review as applicable.
- Identify persistent-format/compatibility changes and required ADR decisions.

## Validation before implementation

- Correctness invariants, independent fixtures and failure/crash boundaries.
- Matched baseline and candidate workloads for performance-sensitive changes;
  report durability settings, environment, raw results and relevant tail metrics.
- Acceptance criteria, rollback/migration behavior and unresolved assumptions.

After implementation, link actual results and deviations. The PR summarizes the
selection, evidence, deliberate differences and remaining unverified items.
