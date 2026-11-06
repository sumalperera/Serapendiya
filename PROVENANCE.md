# Provenance and dependency policy

All application code in this repository is original work for Serapendiya. No
upstream source, generated asset, or copied implementation is included.

G0 dependencies are limited to the packages declared in `Cargo.toml`. Their
exact resolved versions and checksums are committed in `Cargo.lock`. Before
adding a dependency, inspect its registry metadata, API, maintenance status,
licence, and transitive licence information. Do not add a dependency merely
because it is listed in a future design document.

The repository's own code is licensed under MIT. Dependency licence review is
recorded with the verification evidence for the PR that introduces it.
