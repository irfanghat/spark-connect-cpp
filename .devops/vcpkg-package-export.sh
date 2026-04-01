#!/bin/bash


# Install packages

vcpkg install


# export packages

vcpkg export --zip --output-dir=./vcpkg_export --output=vcpkg-package


# extract packages

unzip -o ./vcpkg_export/vcpkg-package.zip -d ./vcpkg_toolchain
