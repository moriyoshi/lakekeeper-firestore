# Container image of the catalog server, after upstream Lakekeeper's docker/full.Dockerfile
# (v0.15.1). It needs BuildKit (buildx), for the build platform below. See DEPLOY.md.
#
#   docker build -t lakekeeper-firestore .                        # without the console
#   docker build --build-arg FEATURES=ui -t lakekeeper-firestore . # with the console at /ui
#
# `--build-arg CARGO_BUILD_JOBS=N` trades memory for speed (see below).
#
# Cloud Run runs linux/amd64: on another host, add `--platform linux/amd64`. The builder
# still runs on the host's own platform and cross-compiles, so nothing runs under emulation.

# Node.js for the builder's own platform: `COPY --from=` an image would take the target's.
FROM --platform=$BUILDPLATFORM node:24-trixie-slim AS node

FROM --platform=$BUILDPLATFORM rust:1.99.1-slim-trixie AS builder
ARG TARGETARCH

# Make crate downloads resilient to transient registry blips, as upstream does.
ENV CARGO_HTTP_MULTIPLEXING=false
ENV CARGO_NET_RETRY=10

# cmake and clang for aws-lc-sys; libssl-dev for native-tls, which the Azure SDK pulls in.
# Cross-compiling adds the target's GCC and its libssl-dev, from Debian's multiarch, and the
# target's Rust standard library. /cross.env sets cargo and the C builds up for the target.
RUN set -e; \
    arch="$TARGETARCH"; \
    case "$arch" in amd64) gnu=x86_64 deb=x86-64 ;; arm64) gnu=aarch64 deb=aarch64 ;; \
        *) echo "unsupported target architecture: $arch" >&2; exit 1 ;; esac; \
    packages="build-essential cmake libclang-dev libssl-dev pkg-config perl"; \
    if [ "$(dpkg --print-architecture)" = "$arch" ]; then \
        : >/cross.env; \
    else \
        dpkg --add-architecture "$arch"; \
        packages="$packages gcc-${deb}-linux-gnu g++-${deb}-linux-gnu libssl-dev:${arch}"; \
        target="${gnu}-unknown-linux-gnu"; \
        rustup target add "$target"; \
        upper=$(echo "$target" | tr a-z- A-Z_); lower=$(echo "$target" | tr - _); \
        { echo "export CARGO_BUILD_TARGET=$target"; \
          echo "export CARGO_TARGET_${upper}_LINKER=${gnu}-linux-gnu-gcc"; \
          echo "export CC_${lower}=${gnu}-linux-gnu-gcc CXX_${lower}=${gnu}-linux-gnu-g++"; \
          echo "export PKG_CONFIG_ALLOW_CROSS=1"; \
          echo "export PKG_CONFIG_LIBDIR_${lower}=/usr/lib/${gnu}-linux-gnu/pkgconfig"; \
        } >/cross.env; \
    fi; \
    apt-get update -qq; \
    DEBIAN_FRONTEND=noninteractive apt-get install -yqq --no-install-recommends $packages; \
    rm -rf /var/lib/apt/lists/*

# Node.js, which the console's build script runs (`npm ci`, `vite build`); unused without `ui`.
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s ../lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm && \
    ln -s ../lib/node_modules/npm/bin/npx-cli.js /usr/local/bin/npx

WORKDIR /app
COPY . .

# Peak memory, measured: about 2.7 GB for the largest rustc process, and about 3 GB for the
# console's `npm ci`, which builds upstream's console-components from git and overlaps the
# first crates. Two jobs keep the build near 4 GB; raise CARGO_BUILD_JOBS on a larger machine.
# The Node heap cap keeps npm's peak the same on any machine (V8's default grows with RAM);
# 1.5 GB is too small for console-components.
ARG CARGO_BUILD_JOBS=2
ENV NODE_OPTIONS=--max-old-space-size=2048

# Cargo features: empty, or `ui` for the console. The cache mounts keep crates and build
# outputs between local builds; a fresh Cloud Build machine starts without them.
ARG FEATURES=""
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/app/target,sharing=locked \
    . /cross.env && \
    CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS}" cargo build --release --locked --features "${FEATURES}" && \
    cp "target/${CARGO_BUILD_TARGET:-.}/release/lakekeeper-firestore" /lakekeeper-firestore

# glibc, libgcc and libssl, which the binary links dynamically. Only COPY runs in this stage,
# so it needs no emulation either.
FROM gcr.io/distroless/cc-debian13:nonroot

COPY --from=builder /lakekeeper-firestore /home/nonroot/lakekeeper-firestore

EXPOSE 8181
ENTRYPOINT ["/home/nonroot/lakekeeper-firestore"]
CMD ["serve"]
