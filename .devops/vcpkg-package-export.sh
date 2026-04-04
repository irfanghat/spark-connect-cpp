#!/bin/bash

set -e

# --------------------------------
# Build and install port packages
# --------------------------------
vcpkg install

vcpkg export --zip --output-dir=./vcpkg_export --output=vcpkg-package

mkdir -p ./vcpkg_toolchain
cp ./vcpkg_export/vcpkg-package.zip ./vcpkg_toolchain/vcpkg-package.zip

unzip -o ./vcpkg_export/vcpkg-package.zip -d ./vcpkg_toolchain