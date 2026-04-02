
    [SPARK-CONNECT][CPP] Sync protocol with Spark v4.1 and refine RuntimeConfig validation
    
    This PR updates the C++ connector to support the Apache Spark v4.1.0 protocol and refines how session configurations are handled to align with Spark 4.x server-side strictness.
    
    Key implementation details include:
    
    * Protocol Sync: Updated all `.proto` definitions (base, commands, relations, etc.) to v4.1.0 and added untracked `ml.proto`, `pipelines.proto`, and `example_plugins.proto`.
    * Builder Overloads: Improve API parity with PySpark.
    * Handle Spark v4's `CANNOT_MODIFY_CONFIG` restrictions for properties like `spark.master`.
    
    Testing performed:
    
    * Suite: Ran entire suite against a standalone Spark 4.x Connect server.
    
    Why is this change necessary?
    Spark v4 introduces stricter validation for session-level properties and new protocol fields.
    Moving to a runtime-first configuration pattern ensures local development remains stable while providing the necessary structures for Spark 4.1 compatibility.
    
    Does this introduce a user-facing change?
    Yes. Users can now customize Spark’s behavior such as memory allocation, shuffle partitions, warehouse location, etc. at the time of building the Spark Session via the builder.config() pattern
:
    
    ```cpp
    auto spark = SparkSession::builder()
        .master("sc://localhost")
        .config("spark.sql.shuffle.partitions", 200)
        .appName("spark-connect-cpp")
        .getOrCreate();
    ```