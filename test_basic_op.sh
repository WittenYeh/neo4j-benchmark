GRAPH_NAME=$1

python neo4j_benchmark.py \
    --command test_basic_op \
    --graph-name $GRAPH_NAME \
    --password neo4j_password