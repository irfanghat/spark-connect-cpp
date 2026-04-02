#!/bin/bash

set -e

echo "Installing CMake, Ninja, and vcpkg..."

sudo apt update
sudo apt install -y \
    build-essential \
    tar \
    curl \
    zip \
    unzip \
    pkg-config \
    git \
    libtool \
    autoconf \
    automake \
    gnupg \
    wget \
    lsb-release \
    flex

echo "Setting up Kitware repository for latest CMake..."
wget -O - https://apt.kitware.com/keys/kitware-archive-latest.asc 2>/dev/null | gpg --dearmor - | sudo tee /usr/share/keyrings/kitware-archive-keyring.gpg >/dev/null

echo "deb [signed-by=/usr/share/keyrings/kitware-archive-keyring.gpg] https://apt.kitware.com/ubuntu/ $(lsb_release -cs) main" | sudo tee /etc/apt/sources.list.d/kitware.list >/dev/null

sudo apt update
sudo apt install -y cmake ninja-build

# -------------------------------------------------
# Setup vcpkg
# -------------------------------------------------
VCPKG_ROOT="$HOME/vcpkg"

if [ -d "$VCPKG_ROOT" ]; then
    echo "directory $VCPKG_ROOT already exists. Skipping clone..."
else
    echo "git cloning vcpkg into $VCPKG_ROOT..."
    git clone https://github.com/microsoft/vcpkg.git "$VCPKG_ROOT"
fi

echo "Bootstrapping vcpkg..."
"$VCPKG_ROOT/bootstrap-vcpkg.sh"

if ! grep -q "VCPKG_ROOT" "$HOME/.bashrc"; then
    echo "Adding VCPKG_ROOT to .bashrc..."
    echo "export VCPKG_ROOT=$VCPKG_ROOT" >> "$HOME/.bashrc"
    echo "export PATH=\$VCPKG_ROOT:\$PATH" >> "$HOME/.bashrc"
fi

echo "-------------------------------------------------------"
echo "Installation complete..."
echo "Please run: 'source ~/.bashrc' to update your current shell."
echo "You can then run 'vcpkg help' to verify."
echo "-------------------------------------------------------"