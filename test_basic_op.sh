GRAPH_NAME=$1

java -jar target/neo4j-benchmark-tool-1.0-SNAPSHOT.jar \
    --uri bolt://localhost:52831 \
    --user neo4j \
    --password neo4j-password \
    --graph-name $GRAPH_NAME \
    --command test_basic_op
