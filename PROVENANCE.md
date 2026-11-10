# Provenance and dependency policy

The repository maintains its own application implementation. Independent test
fixtures generated with kafka-python are identified in the tests and the
verification record; external dependencies are licensed separately. Review the
provenance and license of any material before incorporating it.

G0 dependencies are limited to the packages declared in `Cargo.toml`. Their
exact resolved versions and checksums are committed in `Cargo.lock`. Before
adding a dependency, inspect its registry metadata, API, maintenance status,
licence, and transitive licence information. Do not add a dependency merely
because it is listed in a future design document.

The repository's own code is licensed under MIT. Dependency licence review is
recorded with the verification evidence for the PR that introduces it.

## CRC32C dependency review (2026-11-11)

The storage checksum wrapper uses `crc32c` exactly 0.6.9, obtained from crates.io.
Its registry archive checksum is recorded in Cargo.lock. The crate and its
build dependencies `rustc_version` 0.4.1 and `semver` 1.0.28 declare
`MIT OR Apache-2.0`; the MIT option is selected for this dependency review.
Their manifest/license files were inspected. No upstream source was copied into
this repository. The former local bitwise implementation remains only as a
reference in tests.

Upstream: https://github.com/zowens/crc32c. On review the repository was not
archived; its latest observed commit was
`d33238b6c605c1a421c77218940f3bc5467a87bd`, dated 2026-11-02. The published crate
has no declared rust-version; the project verifies it by running its Rust 1.85
suite. `rustc_version` declares Rust 1.32 and `semver` Rust 1.68. Review of the
RustSec advisory index found no matching entries for these three package names;
this is a limited name check, not a full dependency security audit.

The safe one-shot API dispatches to runtime-detected ARM64 CRC or x86-64 SSE4.2
instructions and otherwise a table-based software implementation. Hardware
branches use upstream unsafe code; that code was not independently audited
here. Target coverage and measurement limits are recorded in
[the verification report](docs/verification/G1-crc-library.md).
