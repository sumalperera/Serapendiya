# G0 closeout review

Review date: 2026-11-08. This report assesses the current checkout against
the v13 G0 contract. G0 cannot yet be declared fully verified: historical PR
evidence and an updated clean Linux CI run remain incomplete.

## Implementation and verification

The workspace, toolchain, lockfile, binaries, domain types, configuration,
tests, CI, provenance policy and ADRs are present. The original local command
results are in [PR03](PR03.md) and [its output](PR03-output.txt). All six
required commands passed on the current code in a Linux aarch64 Docker image;
see [G0 Linux verification](G0-linux-docker.md) for commands, tested commit,
toolchain, lockfile hash, results and limitations. This does not establish the
results for the original PR01–PR03 commits. The CI workflow has been updated
locally to include both CLI checks; a run of that updated workflow remains
UNVERIFIED.

The architecture test checks expected entry points; it is not a comprehensive
enforcement of architectural boundaries. The evidence files for PR01–PR03
do not identify the commits they tested, and their required per-PR details are
incomplete. Do not infer missing historical results from the current run.

## Approvals and scope

ADR-001 records founder acceptance on 2026-11-07; ADR-002 is proposed;
ADR-003 and ADR-004 record founder acceptance on 2026-11-07. See the ADR files
for the decisions and approval records. PR03's statement that all ADRs are
proposed reflects an earlier state. The repository also records subsequent G1
authorization and contains G1 code; see [G1 evidence](G1.md). G1 acceptance
and production durability claims are outside this G0 review.

## Dependency and provenance review

The project declares MIT in Cargo.toml and contains an MIT LICENSE. PROVENANCE.md
states that application code is original. That assertion has not been
independently audited. On 2026-11-08, `cargo metadata --locked --format-version 1`
exited 0 after fetching missing platform-specific packages. Its package
metadata covers the locked dependency graph, including platform-specific
packages. The Cargo.lock SHA-256 was
`5f9e35ccbe672f87d2abff5e4fe29090f26c04b766e80fe9216bd6e442e1b04b`.

The metadata declares the following licence expressions:

- `strsim 0.11.1`, `winnow 0.7.15` and `winnow 1.0.5`: MIT.
- `unicode-ident 1.0.27`: `(MIT OR Apache-2.0) AND Unicode-3.0`.
- All other third-party packages in the resolved metadata: MIT OR Apache-2.0
  (some express the alternatives in reverse order). Exact versions and
  checksums are in Cargo.lock.

This is a review of declared metadata, not a source, licence-text, notice or
legal audit. Unicode-3.0 obligations and distribution notices require review
before publication; the blueprint separately requires approved licensing and
provenance review before source publication.

## Remaining items

1. Recover and review original PR commit and verification evidence where
   available; otherwise retain explicit UNVERIFIED status in PR01–PR03.
2. Run the updated CI workflow on clean Linux and record the run URL, tested
   commit and results. The local Docker run is additional evidence, not a CI run.
3. Complete and approve the publication-level licence and provenance review,
   including third-party notices and Unicode-3.0.
4. Review whether the entry-point architecture test provides the intended
   boundary assurance, and record any accepted limitation.

The contract requested a proposed first G1 PR at G0 closeout. G1 work has
since been authorized and implemented according to the repository records;
its review is the current follow-up, not a proposal to reimplement it.
