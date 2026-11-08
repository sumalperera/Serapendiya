# G0 Linux verification on the current repository

Date: 2026-11-08. Tested commit: `dcb6082cb2d1f5615c97bbdf2205fe9e878383d1`.
The two staged image files in the host working tree are unrelated to the Rust
build and are not copied into the verification image. This run verifies the
current repository, which includes later G1 code; it does not reconstruct the
historical PR01–PR03 states.

Environment: Docker Desktop Linux aarch64, `rust:1.99.1-bookworm` base image
digest `sha256:e6f2cff35d0f309acf8433b4f05878ed77092602c5065cc81801177da0bb3a9c`;
`rustc 1.99.1 (58cf8f9c3 2026-10-13)`;
`cargo 1.99.1 (5f94df478 2026-08-27)`.
Cargo.lock SHA-256: `5f9e35ccbe672f87d2abff5e4fe29090f26c04b766e80fe9216bd6e442e1b04b`.
Direct resolved dependencies: clap 4.6.7, serde 1.0.229, thiserror 2.0.21,
toml 0.9.12+spec-1.1.0; see Cargo.lock for transitive versions.

`docker build --progress plain -f Dockerfile.g1-test -t serapendiya-g0-verification .`
exited 0. Its uncached verification stages reported:

```text
RUN cargo build --locked                              DONE
RUN cargo fmt --all --check                           DONE
RUN cargo clippy --workspace --all-targets -- -D warnings  DONE
RUN cargo test --workspace                            DONE
23 library tests passed; 1 architecture test passed; 2 crash recovery tests passed.
```

Each Dockerfile RUN stage must exit 0 for the image build to succeed. The CLI
checks were then run using:

```sh
docker run --rm serapendiya-g0-verification sh -c 'rustc --version; cargo --version; uname -sm; cargo run --locked --bin streamd -- --version; cargo run --locked --bin streamctl -- --help'
```

The container exited 0 and the relevant output was:

```text
Linux aarch64
streamd 0.1.0
Serapendiya command-line client

Usage: streamctl [COMMAND]

Commands:
  info  Show the client surface available in this build
  help  Print this message or the help of the given subcommand(s)

Options:
  -h, --help     Print help
  -V, --version  Print version
```

Coverage includes domain and configuration unit tests, the entry-point
architecture test, and current G1 storage and recovery tests. The container
run does not establish historical per-PR verification, a GitHub Actions run,
transitive dependency licence review, or production filesystem durability.
Those items remain unverified here. ADR-001, ADR-003 and ADR-004 record founder
acceptance; ADR-002 remains proposed.
