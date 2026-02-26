# Security Policy

## Supported Versions

We prioritize security updates for the current stable release and the previous major version.

## Reporting a Vulnerability

If you believe you have found a security vulnerability, please do not report it via public GitHub issues.

### How to Report

Please report vulnerabilities by opening a **Draft Security Advisory** on this repository or emailing `paulsanganyamawi@gmail.com`.

**Please include the following in your report:**

* A description of the vulnerability and its potential impact.
* Step-by-step instructions to reproduce the issue (PoC code is highly appreciated).
* The version of `spark-connect-cpp` and the operating system/compiler used.

### What to Expect

* **Acknowledgement:** You will receive a response within **48 hours** acknowledging receipt of your report.
* **Updates:** We will provide status updates at least once every **7 days** while the issue is being investigated.
* **Disclosure:** Once a fix is verified, we will coordinate a public disclosure date with you. We follow "Responsible Disclosure" and ask that you do not share the vulnerability publicly until a patch is available.

## Security Best Practices for Users

To keep your Spark sessions secure, we recommend:

* **TLS/SSL:** Always enable TLS when connecting to a remote Spark driver to prevent man-in-the-middle attacks.
* **Token Management:** Use `spark::sql::SparkSession::Builder::token()` carefully. Never hardcode authentication tokens in your C++ source code (See examples: **Databricks Authentication**).
* **Input Sanitization:** While the DSL protects against traditional SQL injection, be cautious when passing raw user input into `spark->sql()` calls.
