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

This project relies on **pkg-config** for dependency management. Contributors are expected to install the required libraries on their local systems and ensure that the corresponding `.pc` files are available for discovery.
 
## IMPORTANT
**Note:** You can use the provided script in **`.devops/install-deps.sh`** to install the required dependencies on supported Linux distributions.

## Initial Environment Setup

Before configuring the project for the first time, you must ensure your local environment has the required build tools.

*   **CMake** - version 3.16 or later is recommended.
*   **Ninja-build** or **Make**
*   **PkgConfig**

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

### 1. Installing Libraries
Contributors are responsible for installing the development versions of required libraries. These must provide `.pc` metadata files for `pkg-config` to provide the necessary compilation and linking flags.

### 2. Adding a New Dependency
If you introduce a new dependency to the project:
*   Verify that the library provides a `pkg-config` (`.pc`) file.
*   Update the `CMakeLists.txt` using the `pkg_check_modules` command.
*   Update the documentation (e.g., `README.md` or this guide) to list the new requirement.

## File Summary

| File                      | Description                                           |
| ------------------------- | ----------------------------------------------------- |
| `.devops/install-deps.sh` | Shell script to install core dependencies on Linux    |

## Recognition

Contributors are added to our [CONTRIBUTORS.md](CONTRIBUTORS.md) file 
and mentioned in release notes for significant contributions.