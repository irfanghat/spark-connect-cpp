# Contributing Guide: Dependency Management

This project uses **vcpkg** in a *Pre-built SDK* model to ensure consistent builds and minimize compilation times for contributors. Please follow the steps below when setting up or modifying project dependencies.


> [!IMPORTANT]
> **Note:** All dependency management steps and scripts are located within the **`.devops`** folder. Ensure your terminal is correctly navigated to this directory.


## Initial Environment Setup

Before configuring the project for the first time, you must ensure your local environment has the required build tools.

1. Run the setup script:
   ```bash
   ./vcpkg-setup.sh
    ```

2.  This will verify or install:
    *   **CMake**
    *   **Ninja-build**
    *   **vcpkg**


## Managing Dependencies

### 1. Adding a New Dependency

All libraries are tracked in the `vcpkg.json` manifest file. To add a new library:

*   Open `vcpkg.json`
*   Add the required package to the `dependencies` list
*   **Note:** Any changes to this file require a new binary export (see below) to ensure the SDK stays up to date

### 2. Exporting the SDK

If you update `vcpkg.json`, you must generate a new binary archive for the team:

*  Run the export script:
    ```bash
    ./vcpkg-package-export.sh
    ```

*  This script builds the dependencies and creates a zip archive inside the `vcpkg_export` folder.

*  The resulting file will be named `vcpkg-package.zip`.

### 3. Local Development Workflow

When you need a new library for a feature you are currently developing, you do not need to publish a release immediately.

*   Update vcpkg.json with the new library.

*   Run the export script:
    ```bash
    ./vcpkg-package-export.sh
    ```

*   Automatic Local Extraction: The export script includes a final step that extracts the newly built binaries directly into your local environment, inside the `vcpkg_toolchain` folder.

This allows you to continue development and consume the new library locally without waiting for an official GitHub Release.

### 3. Exporting the SDK for the 

Once your local development is complete and you are ready to share the changes:

*  Ensure vcpkg-package-export.sh has been run to generate the final vcpkg-package.zip.

*  This zip contains the pre-built binaries including your recent additions.


## Distribution and Consumption

### Distributing via GitHub

To share updated dependencies with the team, we use an automated GitHub Actions pipeline:

*   Update the Version: In your Pull Request, update the SDK_VERSION variable in the GitHub Action YAML file to your desired new version tag (e.g., v2.0.1).

*   Merge to Main/Develop: Once your changes to vcpkg.json are merged into the protected branches, the pipeline triggers automatically.

*   Automated Release: The pipeline runs the export script and automatically uploads the vcpkg-package.zip to the GitHub Releases page under the specified Version Tag

### Updating the Project to Use the New SDK

After the pipeline completes the release, you must update the project configuration to consume it:

*   Open .devops/vcpkg-package.cmake.

*   Update the PACKAGE_VERSION variable to match the new GitHub Release tag.

    ```cmake
    # Update in .devops/vcpkg-package.cmake
    set(PACKAGE_VERSION "vX.Y.Z")
    ```

Once this change is committed, vcpkg-package.cmake will automatically handle downloading and extracting the updated binaries for all other team members during their next CMake configuration.


## File Summary

| File                      | Description                                           |
| ------------------------- | ----------------------------------------------------- |
| `vcpkg-setup.sh`          | Installs required build tools (CMake, Ninja, vcpkg)   |
| `vcpkg.json`              | Manifest file listing current project dependencies    |
| `vcpkg-package-export.sh` | Builds and zips dependencies into `vcpkg-package.zip` |
| `vcpkg-package.zip`       | Final artifact containing pre-built binaries          |
| `vcpkg-package.cmake`     | Downloads the SDK based on `PACKAGE_VERSION`          |
