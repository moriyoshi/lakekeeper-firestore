# Container image of the catalog server, after upstream Lakekeeper's docker/full.Dockerfile
# (v0.15.1), without BuildKit-only syntax so the legacy builder works too. See DEPLOY.md.
#
#   docker build -t lakekeeper-firestore .                        # without the console
#   docker build --build-arg FEATURES=ui -t lakekeeper-firestore . # with the console at /ui
#
# `--build-arg CARGO_BUILD_JOBS=N` trades memory for speed (see below).
#
# Cloud Run runs linux/amd64: on another host, add `--platform linux/amd64`.

FROM rust:1.99.1-slim-trixie AS builder

# Make crate downloads resilient to transient registry blips, as upstream does.
ENV CARGO_HTTP_MULTIPLEXING=false
ENV CARGO_NET_RETRY=10

# cmake and clang for aws-lc-sys; libssl-dev for native-tls, which the Azure SDK pulls in.
RUN apt-get update -qq && \
    DEBIAN_FRONTEND=noninteractive apt-get install -yqq --no-install-recommends \
        build-essential cmake libclang-dev libssl-dev pkg-config perl && \
    rm -rf /var/lib/apt/lists/*

# Node.js, which the console's build script runs (`npm ci`, `vite build`); unused without `ui`.
COPY --from=node:24-trixie-slim /usr/local/bin/node /usr/local/bin/node
COPY --from=node:24-trixie-slim /usr/local/lib/node_modules /usr/local/lib/node_modules
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

# Cargo features: empty, or `ui` for the console.
ARG FEATURES=""
RUN CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS}" cargo build --release --locked --features "${FEATURES}"

# glibc, libgcc and libssl, which the binary links dynamically.
FROM gcr.io/distroless/cc-debian13:nonroot

COPY --from=builder /app/target/release/lakekeeper-firestore /home/nonroot/lakekeeper-firestore

EXPOSE 8181
ENTRYPOINT ["/home/nonroot/lakekeeper-firestore"]
CMD ["serve"]
