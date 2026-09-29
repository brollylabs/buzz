# CatalyzeX pilot image for the Slack Connect bridge (block/buzz#2826).
# Toolchain matches rust-toolchain.toml.
FROM rust:1.95-bookworm AS build
WORKDIR /src
COPY . .
RUN cargo build --release --locked -p buzz-slack-connect-bridge

FROM debian:bookworm-slim
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates \
 && rm -rf /var/lib/apt/lists/*
COPY --from=build /src/target/release/buzz-slack-connect-bridge /usr/local/bin/buzz-slack-connect-bridge
USER 65534:65534
EXPOSE 3100
ENTRYPOINT ["/usr/local/bin/buzz-slack-connect-bridge"]
