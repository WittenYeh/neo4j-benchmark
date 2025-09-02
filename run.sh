#!/bin/bash

# Compile the Java files
mvn clean install

# Run the benchmark
java -cp target/neo4j-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar Neo4jBenchmark "$@"
