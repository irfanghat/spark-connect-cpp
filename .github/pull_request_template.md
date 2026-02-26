## [SPARK-CONNECT][CPP] <Title of your PR>

### Description
<A brief overview of what this PR introduces and the problem it solves.>

### Key Implementation Details
- **Feature/Component Name:** <Description of change>
- **Internal Logic:** <Describe how you handled protobufs, memory, or gRPC stubs>
- **API Changes:** <List any new public methods added to DataFrame or other classes>

### Testing
<Describe how you verified these changes.>
- [ ] New Integration Test: `SparkIntegrationTest.<TestName>`
- [ ] Manual verification via Spark Connect server logs
- [ ] Memory leak check (Valgrind/ASAN)

### Why is this change necessary?
<Explain the motivation, e.g., "To reach feature parity with the PySpark API regarding...">

### User-Facing Changes
**Does this introduce a user-facing change?** (Yes/No)
*If yes, provide a code snippet of the new functionality:*

```cpp
// Example usage here