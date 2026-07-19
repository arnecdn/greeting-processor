# syntax=docker/dockerfile:1

# Comments are provided throughout this file to help you get started.
# If you need more help, visit the Dockerfile reference guide at
# https://docs.docker.com/go/dockerfile-reference/

# Want to help us make this template better? Share your feedback here: https://forms.gle/ybq9Krt8jtBL3iCk7

ARG APP_NAME=greeting-processor

################################################################################
# Create a base stage with all build-time tooling.
ARG RUST_VERSION=1.89-bullseye
FROM docker.io/rust:${RUST_VERSION} AS chef
WORKDIR /app

# Build dependencies shared by recipe/build stages.
RUN apt-get update && apt-get install -y --no-install-recommends cmake && rm -rf /var/lib/apt/lists/*
RUN cargo install --locked cargo-chef

# Plan dependency graph separately to maximize cache hits.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# Build only dependencies from the recipe.
FROM chef AS builder
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=cache,target=/usr/local/cargo/registry \
	--mount=type=cache,target=/app/target \
	cargo chef cook --release --locked --recipe-path recipe.json

# Build the application using the cached dependency layers.
COPY . .
ARG APP_NAME
RUN --mount=type=cache,target=/usr/local/cargo/registry \
	--mount=type=cache,target=/app/target \
	cargo build --locked --release && \
	cp ./target/release/$APP_NAME /usr/bin/server
RUN libdir="$(dpkg-architecture -qDEB_HOST_MULTIARCH)" && \
	mkdir -p "/runtime-libs/lib/${libdir}" && \
	cp -a "/lib/${libdir}/libz.so.1"* "/runtime-libs/lib/${libdir}/"


################################################################################
# Create a new stage for running the application that contains the minimal
# runtime dependencies for the application. This often uses a different base
# image from the build stage where the necessary files are copied from the build
# stage.
#
# Using distroless/cc-debian12 for minimal size while keeping libc and openssl
# support for Kafka operator TLS/network operations. This is ~100x smaller than
# rust:slim-bullseye while maintaining runtime compatibility.
FROM gcr.io/distroless/cc-debian12:nonroot AS final

# Copy the zlib runtime required by rdkafka/libz-sys.
COPY --from=builder /runtime-libs/ /

# Copy the executable from the "build" stage.
COPY --chown=nonroot:nonroot --from=builder /usr/bin/server /usr/bin/server

USER nonroot:nonroot