#!/bin/bash

# ==============================================================================
# Neo4j Comprehensive Benchmark Automation Script (V5 - Final)
#
# Version Features:
# - Resumable: Skips tests for a dataset if its result file already exists.
# - Consistent Environment: Reloads the graph before each modifying test.
# - Optimized Flow: Read-only tests are performed first to reduce reloads.
# - Dual Logging:
#   - All console output (progress, errors) is mirrored to a log-* file.
#   - Clean, final results are saved separately to a result-* file.
#
# How to Force a Rerun:
#   To restart all tests, delete the ./benchmark_reports_resumable directory.
#   rm -rf ./benchmark_reports_resumable
# ==============================================================================

# --- Safety Settings ---
set -e
set -o pipefail

# --- Script Configuration (Please modify for your environment) ---

# 1. Path to the Neo4j Benchmark JAR file
JAR_PATH="./target/neo4j-benchmark-tool-1.0-SNAPSHOT.jar"

# 2. Directory where datasets are located
DATASET_DIR="~/GraphDatasets"   # Please use your path, e.g., ~/GraphDatasets

# 3. Directory to store the generated report files
REPORTS_DIR="./benchmark_reports_resumable"

# 4. Neo4j database connection details
NEO4J_URI="bolt://localhost:19831"   # Default Bolt port for Neo4j
NEO4J_USER="neo4j"
NEO4J_PASS="neo4j-password" # Please use your password

# 5. Define the array of datasets to be tested
DATASETS=(
    "com-dblp.ungraph.txt"
    "com-lj.ungraph.txt"
    "cit-Patents.txt"
    # Commented out by default as it takes a very long time to run
    # "com-orkut.ungraph.txt"
    # "twitter-2010.txt"        
    # "com-friendster.ungraph.txt" 
)

# --- Main Script Logic ---
EXPANDED_DATASET_DIR=$(eval echo ${DATASET_DIR})

if [ ! -f "$JAR_PATH" ]; then
    echo "Error: Benchmark JAR file not found at '$JAR_PATH'"
    echo "Please compile and package the Java project first using 'mvn package'."
    exit 1
fi

mkdir -p "$REPORTS_DIR"

echo "==========================================================="
echo "      Starting Neo4j Benchmark Suite (Resumable)"
echo "==========================================================="
echo "Results will be saved in: $REPORTS_DIR" # Corrected a typo here from "Eesults"
echo ""

for DATASET_FILE in "${DATASETS[@]}"; do
    DATASET_PATH="$EXPANDED_DATASET_DIR/$DATASET_FILE"
    # Sanitize graph name: remove extension and replace hyphens with underscores
    GRAPH_NAME=$(basename "$DATASET_FILE" | sed 's/\..*//' | tr '-' '_')
    
    # Define separate files for detailed logs and clean results
    LOG_FILE="$REPORTS_DIR/log-${GRAPH_NAME}.txt"
    # --- FIX: Renamed variable for clarity to match the Java argument ---
    RESULT_FILE="$REPORTS_DIR/result-${GRAPH_NAME}.txt"

    # --- Resume Check ---
    # The existence of the result file indicates a successful previous run.
    if [ -f "$RESULT_FILE" ]; then
        echo "---------------------------------------------------------------------------------"
        echo ">>> SKIPPING: Result file '$RESULT_FILE' already exists."
        echo ">>> Assuming tests for dataset '$DATASET_FILE' are complete."
        echo ">>> To re-run, please delete the result file or the entire reports directory."
        echo "---------------------------------------------------------------------------------"
        echo ""
        continue # Skip to the next dataset
    fi
    
    echo "*******************************************************************************"
    echo ">>> Starting tests for dataset: $DATASET_FILE (Graph Name: $GRAPH_NAME)"
    echo "**********************************************************************"

    if [ ! -f "$DATASET_PATH" ]; then
        echo "Warning: Data file '$DATASET_PATH' not found, skipping this dataset." >> /dev/stderr
        echo ""
        continue
    fi
    
    # This function executes the java command.
    run_benchmark_command() {
        # --- FIX: Changed --report-path to the correct --result-path argument ---
        # This argument is now required by the Java application.
        java -jar "$@" --result-path "$RESULT_FILE" 2>&1 | tee "$LOG_FILE"
    }
    
    # Overwrite the log file at the beginning of each run for a clean log.
    echo "Neo4j Benchmark FULL LOG for: $GRAPH_NAME" > "$LOG_FILE"
    echo "Test started on: $(date)" >> "$LOG_FILE"
    echo "=================================================" >> "$LOG_FILE"

    # Automatically determine if the graph is undirected
    UNDIRECTED_FLAG=""
    if [[ $DATASET_FILE == *".ungraph."* ]]; then
        UNDIRECTED_FLAG="--undirected"
    fi
    
    # --- Phase 1: Initial Load & Read-Only Algorithm Tests ---
    echo "" # Add spacing for better readability
    echo "--> [Phase 1/3] Performing initial data load..."
    run_benchmark_command "$JAR_PATH" --command load_graph --graph-name "$GRAPH_NAME" --data-path "$DATASET_PATH" --uri "$NEO4J_URI" --user "$NEO4J_USER" --password "$NEO4J_PASS" $UNDIRECTED_FLAG
    
    echo "--> [Phase 1/3] Performing graph algorithm performance tests (read-only)..."
    run_benchmark_command "$JAR_PATH" --command test_algm --graph-name "$GRAPH_NAME" --algm all --uri "$NEO4J_URI" --user "$NEO4J_USER" --password "$NEO4J_PASS"

    # --- Phase 2: Basic Operations Tests (Modifying Operations) ---
    # echo ""
    # echo "--> [Phase 2/3] Reloading a clean graph for 'Basic Operations Test'..."
    # run_benchmark_command "$JAR_PATH" --command load_graph --graph-name "$GRAPH_NAME" --data-path "$DATASET_PATH" --uri "$NEO4J_URI" --user "$NEO4J_USER" --password "$NEO4J_PASS" $UNDIRECTED_FLAG
    
    echo "--> [Phase 2/3] Performing basic operations latency tests..."
    run_benchmark_command "$JAR_PATH" --command test_basic_op --graph-name "$GRAPH_NAME" --uri "$NEO4J_URI" --user "$NEO4J_USER" --password "$NEO4J_PASS"

    # # --- Phase 3: Read/Write Throughput Tests (Modifying Operations) ---
    # echo ""
    # echo "--> [Phase 3/3] Performing read/write throughput tests..."
    # for ratio in 0.2 0.5 0.8; do
    #     echo ""
    #     echo "    -> Reloading graph for test with read ratio ${ratio}..."
    #     run_benchmark_command "$JAR_PATH" --command load_graph --graph-name "$GRAPH_NAME" --data-path "$DATASET_PATH" --uri "$NEO4J_URI" --user "$NEO4J_USER" --password "$NEO4J_PASS" $UNDIRECTED_FLAG

    #     echo "    -> Running throughput test with read ratio ${ratio}..."
    #     run_benchmark_command "$JAR_PATH" --command test_read_write_op --graph-name "$GRAPH_NAME" --read-ratio $ratio --uri "$NEO4J_URI" --user "$NEO4J_USER" --password "$NEO4J_PASS"
    # done

    echo ""
    echo ">>> All tests for dataset '$DATASET_FILE' are complete."
    # --- FIX: Using the renamed variable for consistency ---
    echo ">>> Clean results saved to: $RESULT_FILE"
    echo ">>> Detailed log saved to: $LOG_FILE"
    echo ""
done

echo "==================================================================="
echo "      All benchmark tests have been executed successfully!"
echo "==================================================================="