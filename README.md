# Apache Spark

Apache Spark™ is a multi-language engine for executing data engineering, data science, and machine learning on single-node machines or clusters.

## Running locally

Spark requires the following dependencies:

- Scala 2.12.15
- Apache Spark 3.3.2
- sbt 1.8.3
- jdk-11

Run/debug configurations:

- Add VM options
```
-Dspark.master=local
--add-opens=java.base/java.nio=ALL-UNNAMED
--add-exports
java.base/sun.nio.ch=ALL-UNNAMED
```
- Add dependencies with provided scope to classpath
- Environment variables
```
GOOGLE_APPLICATION_CREDENTIALS=/path/to/key.json
```