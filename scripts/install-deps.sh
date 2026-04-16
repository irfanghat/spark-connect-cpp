#!/bin/bash

set -euo pipefail

echo "----------------------------------------------------------------------"
echo ""
echo "* IMPORTANT *"
echo ""
echo "This script will:"
echo "  - Update APT package lists"
echo "  - Update cmake to the latest stable version"
echo "  - Install build tools and compilers"
echo "  - Install gRPC and Protobuf"
echo "  - Add Apache Arrow APT repository"
echo "  - Install Apache Arrow C++ libraries and related components"
echo ""
echo "System changes:"
echo "  - Requires sudo privileges"
echo "  - Installs packages via apt"
echo ""
echo "Starting setup..."
echo "----------------------------------------------------------------------"
echo ""

echo ""
echo "Installing CMake 4.3.1 (binary)..."
echo ""

CMAKE_VERSION=4.3.1
CMAKE_DIR="cmake-${CMAKE_VERSION}-linux-x86_64"
CMAKE_TARBALL="${CMAKE_DIR}.tar.gz"

wget -q https://github.com/Kitware/CMake/releases/download/v${CMAKE_VERSION}/${CMAKE_TARBALL}

tar -xzf ${CMAKE_TARBALL}

sudo mv ${CMAKE_DIR} /opt/cmake

sudo ln -sf /opt/cmake/bin/cmake /usr/local/bin/cmake
sudo ln -sf /opt/cmake/bin/ctest /usr/local/bin/ctest
sudo ln -sf /opt/cmake/bin/cpack /usr/local/bin/cpack

rm -f ${CMAKE_TARBALL}

echo ""
echo "CMake version:"
cmake --version

sudo apt update

echo ""
echo "Installing ninja..."
sudo apt install ninja-build
echo ""

echo ""
echo "Installing gRPC and Protobuf dependencies..."
echo ""
sudo apt install -y build-essential protobuf-compiler libprotobuf-dev \
                    libgrpc-dev libgrpc++-dev protobuf-compiler-grpc

echo ""
echo "Installing prerequisites for Apache Arrow..."
echo ""
sudo apt install -y -V ca-certificates lsb-release wget gnupg

DISTRO=$(lsb_release --id --short | tr 'A-Z' 'a-z')
CODENAME=$(lsb_release --codename --short)
ARROW_PKG="apache-arrow-apt-source-latest-${CODENAME}.deb"
ARROW_URL="https://packages.apache.org/artifactory/arrow/${DISTRO}/${ARROW_PKG}"

echo ""
echo "Downloading Apache Arrow APT source package..."
wget -q "${ARROW_URL}" -O "${ARROW_PKG}"

echo ""
echo "Installing Apache Arrow APT source package..."
echo ""
sudo apt install -y -V "./${ARROW_PKG}"

echo ""
echo "Updating APT package list (Apache Arrow repos)..."
echo ""
sudo apt update

echo ""
echo "Installing Apache Arrow libraries..."
echo ""
sudo apt install -y -V \
    cmake \
    valgrind \
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

echo ""
echo ""
echo "Cleaning up downloaded package..."
echo ""
rm -f "./${ARROW_PKG}"

echo "All dependencies installed successfully."
echo ""
