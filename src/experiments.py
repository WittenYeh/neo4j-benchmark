# file: experiment.py

import subprocess
import sys
from pathlib import Path
from rich import print as rprint

# --- 脚本配置 (请根据您的环境修改) ---

# 1. 数据集所在的目录
DATASET_DIR = Path("~/GraphDataset").expanduser()

# 2. 生成的报告文件存放的目录
REPORTS_DIR = Path("./benchmark_reports_py_resumable")

# 3. Neo4j数据库连接信息
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "password"  # 请使用您的密码

# 4. 定义要测试的数据集数组
DATASETS = [
    "com-dblp.ungraph.txt",
    "com-lj.ungraph.txt",
    "com-orkut.ungraph.txt",
    "cit-Patents.txt",
    "twitter-2010.txt",
    # "com-friendster.ungraph.txt", # 默认注释掉，因为它运行时间非常长
]

def run_benchmark(args: list[str]):
    """执行一个benchmark命令，如果失败则退出脚本"""
    command = [sys.executable, "neo4j_benchmark.py"] + args
    rprint(f"\n[bold blue]Executing:[/bold blue] {' '.join(command)}")
    
    # 使用 Popen 实时打印输出，更友好
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, encoding='utf-8')
    
    # 实时打印子进程的输出
    for line in iter(process.stdout.readline, ''):
        print(line, end='')

    process.wait() # 等待子进程结束
    if process.returncode != 0:
        rprint(f"\n[bold red]Error: Command failed with exit code {process.returncode}. Aborting.[/bold red]")
        sys.exit(1)

def main():
    """主执行函数"""
    if not DATASET_DIR.is_dir():
        rprint(f"[bold red]Error: Dataset directory not found at '{DATASET_DIR}'[/bold red]")
        sys.exit(1)
    
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    rprint("="*60)
    rprint("[bold green]      开始执行Neo4j基准测试套件 (Python版)[/bold green]")
    rprint("[bold yellow]      支持断点恢复 & 保证测试一致性[/bold yellow]")
    rprint("="*60)
    rprint(f"报告将保存在: {REPORTS_DIR}")

    for dataset_file in DATASETS:
        dataset_path = DATASET_DIR / dataset_file
        # 从文件名中提取一个简短的图名称 (例如: com-dblp)
        graph_name = dataset_path.stem.replace('.ungraph', '').replace('.txt', '')
        report_path = REPORTS_DIR / f"report-{graph_name}.txt"

        # --- 断点恢复检查 ---
        if report_path.exists():
            rprint(f"\n[bold yellow]SKIPPING:[/bold yellow] 报告 '{report_path.name}' 已存在，跳过数据集 '{dataset_file}'.")
            continue

        rprint(f"\n{'*'*70}")
        rprint(f"[bold magenta]>>> 开始测试数据集: {dataset_file} (图名称: {graph_name})[/bold magenta]")
        rprint(f"{'*'*70}")

        if not dataset_path.is_file():
            rprint(f"[bold red]Warning: Data file not found at '{dataset_path}', skipping.[/bold red]")
            continue

        # 通用参数
        base_args = [
            "--graph-name", graph_name,
            "--report-path", str(report_path),
            "--uri", NEO4J_URI, "--user", NEO4J_USER, "--password", NEO4J_PASS
        ]
        
        # 自动判断是否为无向图
        load_args = base_args + ["--data-path", str(dataset_path)]
        if ".ungraph." in dataset_file:
            load_args.append("--undirected")

        # --- Phase 1: 初始加载 & 只读算法测试 ---
        rprint("\n[cyan]--> [Phase 1/3] 正在执行初始数据加载和算法测试...[/cyan]")
        run_benchmark(["--command", "load_graph"] + load_args)
        run_benchmark(["--command", "test_algm"] + base_args)
        
        # --- Phase 2: 基础操作测试 (修改性操作) ---
        rprint("\n[cyan]--> [Phase 2/3] 为 '基础操作测试' 重新加载图并执行测试...[/cyan]")
        run_benchmark(["--command", "load_graph"] + load_args) # 重新加载
        run_benchmark(["--command", "test_basic_op"] + base_args)
        
        # --- Phase 3: 读写吞吐量测试 (修改性操作) ---
        rprint("\n[cyan]--> [Phase 3/3] 正在执行读写吞吐量测试...[/cyan]")
        for ratio in [0.2, 0.5, 0.8]:
            rprint(f"\n[cyan]    -> 为读写比例 {ratio} 的测试重新加载图...[/cyan]")
            run_benchmark(["--command", "load_graph"] + load_args) # 每次都重新加载
            
            rw_args = base_args + ["--read-ratio", str(ratio)]
            run_benchmark(["--command", "test_read_write_op"] + rw_args)
        
        rprint(f"\n[bold green]>>> 数据集 '{dataset_file}' 的所有测试已完成。[/bold green]")
        rprint(f"[bold green]>>> 详细报告已保存至: {report_path}[/bold green]")

    rprint("\n" + "="*60)
    rprint("[bold green]      所有基准测试已成功执行！[/bold green]")
    rprint("="*60)


if __name__ == "__main__":
    main()