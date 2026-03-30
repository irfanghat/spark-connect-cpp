
set(VCPKG_MANIFEST_MODE OFF CACHE BOOL "Disable vcpkg manifest install" FORCE)

set(PACKAGE_VERSION "v1.0.0")
set(PACKAGE_URL "https://github.com/devsimiyu/spark-connect-cpp/releases/download/vcpkg-package-${PACKAGE_VERSION}/vcpkg-package.zip")
set(PACKAGE_DOWNLOAD "${CMAKE_BINARY_DIR}/vcpk-package.zip")


if(NOT EXISTS "${CMAKE_BINARY_DIR}/vcpkg-package")

    message(STATUS "VCPKG PACKAGE not found. Downloading version ${PACKAGE_VERSION}...")
    
    file(DOWNLOAD "${PACKAGE_URL}" "${PACKAGE_DOWNLOAD}" 
        SHOW_PROGRESS 
        TLS_VERIFY ON
    )

    file(ARCHIVE_EXTRACT INPUT "${PACKAGE_DOWNLOAD}" DESTINATION "${CMAKE_BINARY_DIR}")

    file(REMOVE "${PACKAGE_DOWNLOAD}")

endif()
