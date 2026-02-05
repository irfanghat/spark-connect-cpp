#!/bin/bash

set -euo pipefail

echo "[spark-connect-cpp]::[install_deps] ==> Updating APT package list..."
sudo apt update

echo "[spark-connect-cpp]::[install_deps]  ==> Installing gRPC and Protobuf dependencies..."
sudo apt install -y build-essential protobuf-compiler libprotobuf-dev \
                    libgrpc-dev libgrpc++-dev protobuf-compiler-grpc

echo "[spark-connect-cpp]::[install_deps] ==> Installing prerequisites for Apache Arrow..."
sudo apt install -y -V ca-certificates lsb-release wget gnupg

DISTRO=$(lsb_release --id --short | tr 'A-Z' 'a-z')
CODENAME=$(lsb_release --codename --short)
ARROW_PKG="apache-arrow-apt-source-latest-${CODENAME}.deb"
ARROW_URL="https://packages.apache.org/artifactory/arrow/${DISTRO}/${ARROW_PKG}"

echo "[spark-connect-cpp]::[install_deps] ==> Downloading Apache Arrow APT source package..."
wget -q "${ARROW_URL}" -O "${ARROW_PKG}"

echo "[spark-connect-cpp]::[install_deps] ==> Installing Apache Arrow APT source package..."
sudo apt install -y -V "./${ARROW_PKG}"

echo "[spark-connect-cpp]::[install_deps] ==> Updating APT package list (Apache Arrow repos)..."
sudo apt update

echo "[spark-connect-cpp]::[install_deps] ==> Installing Apache Arrow libraries..."
sudo apt install -y -V \
    cmake \
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
    libparquet-glib-dev \
    libgtest-dev \
    libgmock-dev

echo "[spark-connect-cpp]::[install_deps] ==> Cleaning up downloaded package..."
rm -f "./${ARROW_PKG}"

echo "[spark-connect-cpp]::[install_deps] ==> All dependencies installed successfully."