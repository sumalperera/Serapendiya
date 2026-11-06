# ADR-001: Original minimal workspace

Status: **ACCEPTED**

## Decision under review

Use one small library crate with two binaries (`streamd` and `streamctl`) for
G0. Keep domain types and configuration independent of process startup code.
The workspace may be split only when an implemented boundary justifies it.

## Alternatives

- Create the full future multi-crate layout immediately: rejected for G0 because
  empty or speculative crates obscure authorization boundaries.
- Fork an existing broker: rejected because G0 requires original code and no
  broker capability is authorized.

## Consequences

This keeps the first gate reviewable and makes the CLI/config boundary
testable.

## Approval record

Approved by founder in the project conversation on 2026-11-07. This approval
authorizes the accepted workspace boundary and does not authorize unrelated
future capabilities.
