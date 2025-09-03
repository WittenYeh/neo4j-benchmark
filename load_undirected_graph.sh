GRAPH_NAME=$1
DATA_PATH=$2

python ./src/neo4j_benchmark.py \
    --command load_graph \
    --graph-name $GRAPH_NAME \
    --data-path $DATA_PATH \
    --password neo4j-password \
    --undirected