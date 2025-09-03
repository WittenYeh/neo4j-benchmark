#!/bin/bash

# ==============================================================================
# Neo4j 全方位基准测试自动化脚本 (V3)
#
# 版本特性:
# - 支持断点恢复：如果报告文件已存在，则跳过该数据集的测试。
# - 为每一次修改性测试重新加载图，确保测试环境的一致性。
# - 优化了测试流程，将只读测试前置，减少不必要的重复加载。
# - 增强了控制台和报告文件的输出信息，使其更清晰、易于追踪。
#
# 如何强制重跑:
#   若要从头开始所有测试，请在运行此脚本前删除报告目录。
#   rm -rf ./benchmark_reports_resumable
# ==============================================================================

# --- 安全设置 ---
set -e
set -o pipefail

# --- 脚本配置 (请根据您的环境修改) ---

# 1. Neo4j Benchmark JAR包的路径
JAR_PATH="./target/neo4j-benchmark-1.0-SNAPSHOT-jar-with-dependencies.jar"

# 2. 数据集所在的目录
DATASET_DIR="~/GraphDataset"

# 3. 生成的报告文件存放的目录
REPORTS_DIR="./benchmark_reports_resumable" # 使用新目录

# 4. Neo4j数据库连接信息
NEO4J_URI="bolt://localhost:7687"
NEO4J_USER="neo4j"
NEO4J_PASS="password" # 请使用您的密码

# 5. 定义要测试的数据集数组
DATASETS=(
    "com-dblp.ungraph.txt"
    "com-lj.ungraph.txt"
    "com-orkut.ungraph.txt"
    "cit-Patents.txt"
    "twitter-2010.txt"
    # "com-friendster.ungraph.txt" # 默认注释掉，因为它运行时间非常长
)

# --- 辅助函数 ---
run_benchmark_command() {
    # 将标准输出和标准错误都重定向到报告文件
    java -jar "$@" >> "$REPORT_FILE" 2>&1
}

# --- 脚本主逻辑 ---
EXPANDED_DATASET_DIR=$(eval echo ${DATASET_DIR})

if [ ! -f "$JAR_PATH" ]; then
    echo "错误: Benchmark JAR文件未找到于 '$JAR_PATH'"
    echo "请先使用 'mvn package' 命令编译和打包Java项目。"
    exit 1
fi

mkdir -p "$REPORTS_DIR"

echo "================================================="
echo "      开始执行Neo4j基准测试套件 (支持断点恢复)"
echo "================================================="
echo "报告将保存在: $REPORTS_DIR"
echo ""

for DATASET_FILE in "${DATASETS[@]}"; do
    DATASET_PATH="$EXPANDED_DATASET_DIR/$DATASET_FILE"
    GRAPH_NAME=$(basename "$DATASET_FILE" | sed 's/\..*//')
    REPORT_FILE="$REPORTS_DIR/report-${GRAPH_NAME}.txt"

    # --- 断点恢复检查 ---
    if [ -f "$REPORT_FILE" ]; then
        echo "----------------------------------------------------------------------"
        echo ">>> SKIPPING: 报告文件 '$REPORT_FILE' 已存在。"
        echo ">>> 假设对数据集 '$DATASET_FILE' 的测试已完成。"
        echo ">>> 如需重新测试，请先删除该报告文件或整个报告目录。"
        echo "----------------------------------------------------------------------"
        echo ""
        continue # 跳到下一个数据集
    fi
    
    echo "**********************************************************************"
    echo ">>> 开始测试数据集: $DATASET_FILE (图名称: $GRAPH_NAME)"
    echo "**********************************************************************"

    if [ ! -f "$DATASET_PATH" ]; then
        echo "警告: 数据文件 '$DATASET_PATH' 不存在，跳过此数据集。" >> /dev/stderr
        echo ""
        continue
    fi
    
    # 初始化报告文件
    echo "Neo4j Benchmark Report for: $GRAPH_NAME (Consistent & Resumable Test Run)" > "$REPORT_FILE"
    echo "Test executed on: $(date)" >> "$REPORT_FILE"
    echo "=================================================" >> "$REPORT_FILE"

    # 自动判断图是否为无向图
    UNDIRECTED_FLAG=""
    if [[ $DATASET_FILE == *".ungraph."* ]]; then
        UNDIRECTED_FLAG="--undirected"
    fi
    
    # --- Phase 1: 初始加载 & 只读算法测试 ---
    echo ""
    echo "--> [Phase 1/3] 正在执行初始数据加载..."
    echo -e "\n\n===== PHASE 1: INITIAL GRAPH LOAD =====\n" | tee -a "$REPORT_FILE"
    run_benchmark_command "$JAR_PATH" --command load_graph --graph-name "$GRAPH_NAME" --data-path "$DATASET_PATH" --uri "$NEO4J_URI" --user "$NEO4J_USER" --password "$NEO4J_PASS" $UNDIRECTED_FLAG
    echo "初始加载完成。"
    
    echo "--> [Phase 1/3] 正在执行图算法性能测试 (只读操作)..."
    echo -e "\n\n===== PHASE 1: GRAPH ALGORITHM (GDS) TEST =====\n" | tee -a "$REPORT_FILE"
    run_benchmark_command "$JAR_PATH" --command test_algm --graph-name "$GRAPH_NAME" --algm all --uri "$NEO4J_URI" --user "$NEO4J_USER" --password "$NEO4J_PASS"
    echo "图算法测试完成。"

    # --- Phase 2: 基础操作测试 (修改性操作) ---
    echo ""
    echo "--> [Phase 2/3] 为 '基础操作测试' 重新加载干净的图..."
    echo -e "\n\n===== RELOADING GRAPH FOR CONSISTENT BASIC OP TEST =====\n" | tee -a "$REPORT_FILE"
    run_benchmark_command "$JAR_PATH" --command load_graph --graph-name "$GRAPH_NAME" --data-path "$DATASET_PATH" --uri "$NEO4J_URI" --user "$NEO4J_USER" --password "$NEO4J_PASS" $UNDIRECTED_FLAG
    echo "图已重新加载。"
    
    echo "--> [Phase 2/3] 正在执行基础操作延迟测试..."
    echo -e "\n\n===== PHASE 2: BASIC OPERATIONS LATENCY TEST =====\n" | tee -a "$REPORT_FILE"
    run_benchmark_command "$JAR_PATH" --command test_basic_op --graph-name "$GRAPH_NAME" --uri "$NEO4J_URI" --user "$NEO4J_USER" --password "$NEO4J_PASS"
    echo "基础操作测试完成。"

    # --- Phase 3: 读写吞吐量测试 (修改性操作) ---
    echo ""
    echo "--> [Phase 3/3] 正在执行读写吞吐量测试..."
    for ratio in 0.2 0.5 0.8; do
        echo ""
        echo "    -> 为读写比例 ${ratio} 的测试重新加载图..."
        echo -e "\n\n===== RELOADING GRAPH FOR CONSISTENT THROUGHPUT TEST (Read Ratio: $ratio) =====\n" | tee -a "$REPORT_FILE"
        run_benchmark_command "$JAR_PATH" --command load_graph --graph-name "$GRAPH_NAME" --data-path "$DATASET_PATH" --uri "$NEO4J_URI" --user "$NEO4J_USER" --password "$NEO4J_PASS" $UNDIRECTED_FLAG
        echo "    -> 图已重新加载。"

        echo "    -> 正在运行读写比例为 ${ratio} 的吞吐量测试..."
        echo -e "\n\n===== PHASE 3: READ/WRITE THROUGHPUT TEST (Read Ratio: $ratio) =====\n" | tee -a "$REPORT_FILE"
        run_benchmark_command "$JAR_PATH" --command test_read_write_op --graph-name "$GRAPH_NAME" --read-ratio $ratio --uri "$NEO4J_URI" --user "$NEO4J_USER" --password "$NEO4J_PASS"
        echo "    -> 读写比例 ${ratio} 的测试完成。"
    done
    echo "所有读写吞吐量测试完成。"

    echo ""
    echo ">>> 数据集 '$DATASET_FILE' 的所有测试已完成。"
    echo ">>> 详细报告已保存至: $REPORT_FILE"
    echo ""
done

echo "================================================="
echo "      所有基准测试已成功执行！"
echo "================================================="