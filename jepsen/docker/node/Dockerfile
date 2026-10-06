# Jepsen node image for Switchboard.
#
# Stage 1 builds the broker for linux/amd64 from source. Stage 2 is the
# actual Jepsen node: Ubuntu with sshd (jepsen.control), iptables (partition
# nemesis), and the switchboard binary. The container runs sshd in the
# foreground; Jepsen starts/stops/kills switchboard over SSH, so killing the
# broker never kills the container.

FROM rust:1 AS build

# clang/libclang: bindgen (librocksdb-sys). cmake/perl/go: aws-lc-sys.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      clang libclang-dev cmake perl golang-go pkg-config \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src
COPY Cargo.toml Cargo.lock ./
COPY .cargo ./.cargo
COPY crates ./crates

# Dependency prebuild: stub every workspace member's sources so this layer's
# target/ cache (all third-party deps incl. rocksdb) survives source edits.
RUN for m in switchboard switchboard-wire switchboard-core switchboard-store switchboard-cluster switchboard-server; do \
      p=crates/$m/src; mkdir -p $p; \
      if [ "$m" = "switchboard" ]; then echo 'fn main(){}' > $p/main.rs; else echo '' > $p/lib.rs; fi; \
    done \
 && cargo build --release --bin switchboard || true

# Real sources overwrite the stubs; only workspace members recompile.
COPY crates ./crates
RUN touch crates/*/src/*.rs && cargo build --release --bin switchboard

FROM ubuntu:24.04

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      openssh-server iptables procps iproute2 netcat-openbsd \
      ca-certificates sudo \
 && rm -rf /var/lib/apt/lists/* \
 && mkdir -p /var/run/sshd \
 && sed -i -E 's/^#?PermitRootLogin.*/PermitRootLogin prohibit-password/' /etc/ssh/sshd_config \
 && sed -i -E 's/^#?UsePAM.*/UsePAM no/' /etc/ssh/sshd_config

COPY --from=build /src/target/release/switchboard /usr/local/bin/switchboard
COPY sb-start sb-watchdog /usr/local/bin/
COPY sshd-key.pub /root/.ssh/authorized_keys

RUN chmod 700 /root/.ssh && chmod 600 /root/.ssh/authorized_keys \
 && chmod +x /usr/local/bin/sb-start

# NET_ADMIN is granted at `docker run` time (partition nemesis needs iptables).
EXPOSE 22 5672 5673
# The watchdog keeps the broker alive across crashes and kill
# nemesis events; sshd is how Jepsen controls everything.
CMD ["sh", "-c", "/usr/local/bin/sb-watchdog & exec /usr/sbin/sshd -D -e"]
