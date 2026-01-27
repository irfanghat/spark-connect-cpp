FROM debian:12-slim

WORKDIR /workspace

RUN apt-get update && apt-get install -y \
    build-essential \
    cmake \
    pkg-config \
    libgtest-dev \
    libgmock-dev \
    protobuf-compiler \
    libprotobuf-dev \
    libgrpc-dev \
    libgrpc++-dev \
    protobuf-compiler-grpc \
    uuid-dev \
    libabsl-dev \
    ca-certificates \
    lsb-release \
    wget \
    netcat-traditional \
    gnupg \
    && rm -rf /var/lib/apt/lists/*

RUN DISTRO=$(lsb_release --id --short | tr 'A-Z' 'a-z') && \
    CODENAME=$(lsb_release --codename --short) && \
    ARROW_PKG="apache-arrow-apt-source-latest-${CODENAME}.deb" && \
    ARROW_URL="https://packages.apache.org/artifactory/arrow/${DISTRO}/${ARROW_PKG}" && \
    echo "Downloading from: ${ARROW_URL}" && \
    wget -q "${ARROW_URL}" -O "${ARROW_PKG}" && \
    apt-get update && \
    apt-get install -y -V "./${ARROW_PKG}" && \
    apt-get update && \
    apt-get install -y -V \
        libarrow-dev \
        libarrow-glib-dev \
        libarrow-dataset-dev \
        libarrow-dataset-glib-dev \
        libarrow-acero-dev \
        libarrow-flight-dev \
        libarrow-flight-glib-dev \
        libarrow-flight-sql-dev \
        libarrow-flight-sql-glib-dev \
        libgandiva-dev \
        libgandiva-glib-dev \
        libparquet-dev \
        libparquet-glib-dev && \
    rm -f "./${ARROW_PKG}" && \
    rm -rf /var/lib/apt/lists/*

COPY . .

RUN mkdir -p build