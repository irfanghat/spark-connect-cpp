# Contributing Guide

First off, thank you for considering contributing to Spark Connect C++.

## Table of Contents

- [Code of Conduct](#code-of-conduct)
- [How Can I Contribute?](#how-can-i-contribute)
- [Development Setup](#setup)

## Code of Conduct

This project and everyone participating in it is governed by our 
[Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are 
expected to uphold this code.

## How Can I Contribute?

### Reporting Bugs

Before creating bug reports, please check existing issues to avoid 
duplicates. When you create a bug report, include as many details as 
possible using our [bug report template](.github/ISSUE_TEMPLATE/bug_report.md).

**Great bug reports include:**
- A clear, descriptive title
- Steps to reproduce the behavior
- Expected behavior vs actual behavior
- Screenshots (if applicable)
- Environment details (OS, versions)

### Suggesting Features

Feature requests are welcome! Please use our 
[feature request template](.github/ISSUE_TEMPLATE/feature_request.md).

**Great feature requests include:**
- Clear problem statement: "I'm frustrated when..."
- Proposed solution
- Alternative solutions you've considered
- Additional context

### Improving Documentation

Documentation improvements are always welcome! This includes:
- Fixing typos
- Adding examples
- Clarifying confusing sections
- Translating documentation

### Submitting Code

Look for issues labeled `good first issue` or `help wanted` for 
great places to start. 

#### IMPORTANT

Exercise cautious use of AI tools. 
**AI generated code** should be **thoroughly reviewed** to ensure
standards are maintained.

## Development Setup

### Prerequisites

- Docker
- Cmake
- Git
- gRPC
- Protobuf
- Apache Arrow
- uuid

## Pull Request Process

### Before Submitting

1. **Update your branch** with the latest upstream changes:
   ```bash
   git fetch upstream
   git rebase upstream/main
   ```

2. **Run the full test suite** and ensure all tests pass:
   ```bash
   ctest --preset <preset-name>
   ```

3. **Update documentation** if you've changed APIs or added features.

### Submitting

1. Push your branch to your fork:
   ```bash
   git push origin feature/your-feature-name
   ```

2. Open a Pull Request against the `main` branch.

3. Fill out the PR template completely.

4. Wait for review. We aim to respond within 7 days.

### PR Checklist

- [ ] My code follows the project's style guidelines
- [ ] I have performed a self-review of my own code
- [ ] I have commented my code, particularly in hard-to-understand areas
- [ ] I have made corresponding changes to the documentation
- [ ] My changes generate no new warnings
- [ ] I have added tests that prove my fix is effective or that my feature works
- [ ] New and existing unit tests pass locally with my changes

## Style Guide

We use the [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html)

### Commit Messages

We follow [Conventional Commits](https://conventionalcommits.org/):

```
[SPARK-CONNECT][CPP](type) <Summarize changes in one line>

<Detailed description of the PR/Implementation>

Key implementation details include:
- <Detail 1>
- <Detail 2>

Testing performed:
- <Test scenario 1>
- <Test scenario 2>

Why is this change necessary?
<Rationale>

Does this introduce a user-facing change?
<Example usage or "No">
```

**Types:**
- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation only
- `style`: Formatting, missing semicolons, etc.
- `refactor`: Code change that neither fixes a bug nor adds a feature
- `test`: Adding missing tests
- `chore`: Maintenance tasks

### Code Style

- Write self-documenting code with meaningful variable names
- Add C++ comments for public APIs
- See: [Google C++ Style Guide](https://google.github.io/styleguide/cppguide.html)

### Testing

- All new features must include tests
- Bug fixes should include regression tests
- Aim for >80% code coverage on new code
- Tests should be deterministic (no flaky tests)


This project uses **vcpkg** in a *Pre-built SDK* model to ensure consistent builds and minimize compilation times for contributors. Please follow the steps below when setting up or modifying project dependencies.


## IMPORTANT
**Note:** All dependency management steps and scripts are located within the **`.devops`** folder. Ensure your terminal is correctly navigated to this directory.


## Initial Environment Setup

Before configuring the project for the first time, you must ensure your local environment has the required build tools.

1. Run the setup script:
   ```bash
   chmod u+x ./vcpkg-setup.sh
   ./vcpkg-setup.sh
    ```

2.  This will verify or install:
    *   **CMake**
    *   **Ninja-build**
    *   **vcpkg**

## Development Environment Setup

To ensure a consistent development experience, we use **CMake Presets** to manage our build configurations. Follow the steps below to set up your local environment:

### 1. Configure Local Presets
While `CMakePresets.json` serves as the primary configuration for our CI/CD pull request pipelines, you should use a local preset for development.

* Locate the `CMakeUserPresets.template.json` file in the root directory.
* Copy this file and save it as **`CMakeUserPresets.json`**.
* The defaults provided in the template are pre-configured to work out-of-the-box for local development.

### 2. Build the Project
Once your `CMakeUserPresets.json` is in place, run the following commands in your terminal to configure and build the project:

```bash
# Select the local configuration preset
cmake --preset local

# Execute the build using the local preset
cmake --build --preset local
```

---

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

*   **Automatic Local Extraction**: The export script includes a final step that extracts the newly built binaries directly into your local environment, inside the `vcpkg_toolchain` folder.

This allows you to continue development and consume the new library locally without waiting for an official GitHub Release.

### 3. Exporting the SDK

Once your local development is complete and you are ready to share the changes:

*  Ensure `vcpkg-package-export.sh` has been run to generate the final `vcpkg-package.zip`.

*  This zip contains the pre-built binaries including your recent additions.


## Distribution and Consumption

### Distributing via GitHub

To share updated dependencies with the team, we use an automated GitHub Actions pipeline:

*   **Update the Version**: In your Pull Request, update the `SDK_VERSION` variable in the GitHub Action YAML file to your desired new version tag (e.g., v2.0.1).

*   **Merge to `main/dev/branch-v.x.x`**: Once your changes to `vcpkg.json` are merged into the protected branches, the pipeline triggers automatically.

*   **Automated Release**: The pipeline runs the export script and automatically uploads the `vcpkg-package.zip` to the GitHub Releases page under the specified Version Tag

### Updating the Project to Use the New SDK

After the pipeline completes the release, you must update the project configuration to consume it:

*   Open .devops/vcpkg-package.cmake.

*   Update the PACKAGE_VERSION variable to match the new GitHub Release tag.

    ```cmake
    # Update in .devops/vcpkg-package.cmake
    set(PACKAGE_VERSION "vX.Y.Z")
    ```

Once this change is committed, `vcpkg-package.cmake` will automatically handle downloading and extracting the updated binaries for all other team members during their next CMake configuration.


## File Summary

| File                      | Description                                           |
| ------------------------- | ----------------------------------------------------- |
| `vcpkg-setup.sh`          | Installs required build tools (CMake, Ninja, vcpkg)   |
| `vcpkg.json`              | Manifest file listing current project dependencies    |
| `vcpkg-package-export.sh` | Builds and zips dependencies into `vcpkg-package.zip` |
| `vcpkg-package.zip`       | Final artifact containing pre-built binaries          |
| `vcpkg-package.cmake`     | Downloads the SDK based on `PACKAGE_VERSION`          |

## Recognition

Contributors are added to our [CONTRIBUTORS.md](CONTRIBUTORS.md) file 
and mentioned in release notes for significant contributions.