# Switchboard soak-lab image: broker + soak driver + k8s helpers, for
# linux/amd64 k3s. Layering for fast rebuilds: member *manifests* only
# seed the workspace, stub sources compile all third-party deps
# (rocksdb, aws-lc-rs) into a cached layer, and real sources — the only
# thing that changes between builds — recompile just the workspace.
#
#   (from the repo root)
#   docker buildx build --platform linux/amd64 -f k8s/Dockerfile -t switchboard-soak-lab:dev .

FROM rust:1 AS build

# clang/libclang: bindgen (librocksdb-sys). cmake/perl/go: aws-lc-sys.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      clang libclang-dev cmake perl golang-go pkg-config \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
# Member manifests only (cheap, rarely changing layer).
COPY crates/switchboard/Cargo.toml crates/switchboard/Cargo.toml
COPY crates/switchboard-wire/Cargo.toml crates/switchboard-wire/Cargo.toml
COPY crates/switchboard-core/Cargo.toml crates/switchboard-core/Cargo.toml
COPY crates/switchboard-store/Cargo.toml crates/switchboard-store/Cargo.toml
COPY crates/switchboard-cluster/Cargo.toml crates/switchboard-cluster/Cargo.toml
COPY crates/switchboard-server/Cargo.toml crates/switchboard-server/Cargo.toml
COPY crates/switchboard-bench/Cargo.toml crates/switchboard-bench/Cargo.toml

# Stub every member's sources and prebuild all third-party deps. This
# layer only re-runs when a Cargo.toml / Cargo.lock changes.
RUN for m in switchboard switchboard-wire switchboard-core switchboard-store switchboard-cluster switchboard-server switchboard-bench; do \
      p=crates/$m/src; mkdir -p $p; rm -rf $p/bin; \
      case $m in \
        switchboard|switchboard-bench) echo 'fn main(){}' > $p/main.rs ;; \
        *) echo '' > $p/lib.rs ;; \
      esac; \
    done \
 && cargo build --release --bin switchboard --bin switchboard-soak || true

# Real sources; only workspace crates recompile.
COPY crates ./crates
RUN touch crates/*/src/*.rs crates/switchboard-bench/src/bin/*.rs \
 && cargo build --release --bin switchboard --bin switchboard-soak

FROM ubuntu:24.04

RUN apt-get update \
 && apt-get install -y --no-install-recommends bash ca-certificates \
 && rm -rf /var/lib/apt/lists/*

COPY --from=build /src/target/release/switchboard /usr/local/bin/
COPY --from=build /src/target/release/switchboard-soak /usr/local/bin/
COPY k8s/sb-k8s-entrypoint k8s/sb-ready /usr/local/bin/
RUN chmod +x /usr/local/bin/sb-k8s-entrypoint /usr/local/bin/sb-ready
