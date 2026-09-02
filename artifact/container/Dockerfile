FROM rust:1.97.0-slim-bookworm@sha256:6d220bf85c74e842a79da63997af8d2e74455c0b8847d8bb3a5888572334991d AS builder

WORKDIR /src

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        build-essential \
        ca-certificates \
        cmake \
        git \
        libusb-1.0-0-dev \
        pkg-config \
    && rm -rf /var/lib/apt/lists/*

RUN git clone --recurse-submodules https://github.com/nlesc-recruit/PowerSensor3.git /tmp/PowerSensor3 \
    && cmake -S /tmp/PowerSensor3/host -B /tmp/PowerSensor3/host/build \
    && cmake --build /tmp/PowerSensor3/host/build \
    && cmake --install /tmp/PowerSensor3/host/build \
    && rm -rf /tmp/PowerSensor3

COPY Cargo.toml Cargo.lock build.rs rust-toolchain.toml ./
COPY src/ src/
COPY app/ app/
COPY common/ common/
COPY macros/ macros/
COPY benches/ benches/
COPY sensors/ sensors/
COPY plots/ plots/
COPY artifact/container/setup.replay.toml setup.toml
COPY artifact/container/Cargo.replay.lock /tmp/Cargo.replay.lock

RUN cargo build --release --package setup \
    && cp /tmp/Cargo.replay.lock Cargo.lock \
    && cargo build --release --package nvme-energy-bench


FROM python:3.12.11-slim-bookworm@sha256:519591d6871b7bc437060736b9f7456b8731f1499a57e22e6c285135ae657bf7

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        libstdc++6 \
    && rm -rf /var/lib/apt/lists/*

COPY artifact/container/requirements.txt /opt/ae/requirements.txt
RUN python -m pip install --no-cache-dir --requirement /opt/ae/requirements.txt

COPY --from=builder /src/target/release/nvme-energy-bench /usr/local/bin/nvme-energy-bench
COPY --from=builder /src/plots/ /opt/nvme-energy-bench/plots/
COPY artifact/container/iiswc-paper-plots/ /opt/paper-plots/

COPY artifact/container/replay.py /opt/ae/

ENTRYPOINT ["python", "/opt/ae/replay.py"]
