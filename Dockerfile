# Multi-stage Dockerfile:
# The `builder` stage compiles the binary and gathers all dependencies in the `/export/` directory.
FROM debian:12 AS builder
RUN apt-get update && apt-get -y upgrade \
 && apt-get -y install wget curl build-essential gcc make libssl-dev pkg-config fuse git

# Install the latest Rust build environment.
RUN curl https://sh.rustup.rs -sSf | bash -s -- -y
ENV PATH="/root/.cargo/bin:${PATH}"

# Install the `depres` utility for dependency resolution.
RUN cd /usr/local/src/ \
 && git clone https://github.com/rrauch/depres.git \
 && cd depres \
 && git checkout 717d0098751024c1282d42c2ee6973e6b53002dc \
 && cargo build --release \
 && cp target/release/depres /usr/local/bin/

COPY Cargo.* /usr/local/src/sia_nfs/
COPY cachalot /usr/local/src/sia_nfs/cachalot/
COPY main /usr/local/src/sia_nfs/main/

# Build the `sia_nfs` binary.
RUN cd /usr/local/src/sia_nfs/ \
 && cargo build --release \
 && cp ./target/release/sia_nfs /usr/local/bin/

# Use `depres` to identify all required files for the final image.
RUN depres /bin/sh /bin/bash /bin/ls /usr/local/bin/sia_nfs \
    /etc/ssl/certs/ \
    /usr/share/ca-certificates/ \
    >> /tmp/export.list

# Copy all required files into the `/export/` directory.
RUN cat /tmp/export.list \
 # remove all duplicates
 && cat /tmp/export.list | sort -o /tmp/export.list -u - \
 && mkdir -p /export/ \
 && rm -rf /export/* \
 # copying all necessary files
 && cat /tmp/export.list | xargs cp -a --parents -t /export/ \
 && mkdir -p /export/tmp && chmod 0777 /export/tmp


# The final stage creates a minimal image with all necessary files.
FROM scratch
WORKDIR /

# Copy files from the `builder` stage.
COPY --from=builder /export/ /

VOLUME /config
EXPOSE 12000
ENV DATA_DIR="/config/"
ENV LISTEN_ADDRESS="0.0.0.0:12000"

ENTRYPOINT ["/usr/local/bin/sia_nfs"]
