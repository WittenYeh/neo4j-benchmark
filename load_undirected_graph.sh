GRAPH_NAME=$1
DATA_PATH=$2

java -jar target/neo4j-benchmark-tool-1.0-SNAPSHOT.jar \
    --uri bolt://localhost:52831 \
    --user neo4j \
    --password neo4j-password \
    --graph-name $GRAPH_NAME \
    --command load_graph \
    --data-path $DATA_PATH \
    --undirected
    