# file: neo4j_benchmark.py

import typer
from enum import Enum
from benchmark_runner import BenchmarkRunner

app = typer.Typer(
    name="neo4j-benchmark",
    help="A benchmark tool for Neo4j, mirroring the Nebula Graph test suite.",
    add_completion=False
)

class CommandType(str, Enum):
    load_graph = "load_graph"
    # load_property = "load_property"
    test_basic_op = "test_basic_op"
    test_read_write_op = "test_read_write_op"
    # test_property = "test_property"
    test_algm = "test_algm"

@app.command()
def run(
    command: CommandType = typer.Option(..., "--command", help="The command to execute.", case_sensitive=False),
    graph_name: str = typer.Option(..., "--graph-name", help="The name of the graph to be tested."),
    report_path: str = typer.Option(None, "--report-path", help="Path to save the detailed benchmark report file."),
    uri: str = typer.Option("bolt://localhost:7687", "--uri", help="Neo4j Bolt URI."),
    user: str = typer.Option("neo4j", "--user", help="Neo4j username."),
    password: str = typer.Option("password", "--password", help="Neo4j password."),
    data_path: str = typer.Option(None, "--data-path", help="The path of the data file to be loaded."),
    undirected: bool = typer.Option(False, "--undirected", help="Whether the graph is undirected."),
    read_ratio: float = typer.Option(0.5, "--read-ratio", help="The ratio of read/lookup operations."),
    algm: str = typer.Option("all", "--algm", help="The algorithm to be tested.")
):
    """
    Run a specific benchmark command for Neo4j.
    """
    try:
        with BenchmarkRunner(uri, user, password, graph_name, report_path) as benchmarker:
            if command == CommandType.load_graph:
                if not data_path:
                    typer.echo("Error: --data-path is required for --command load_graph", err=True)
                    raise typer.Exit(code=1)
                benchmarker.drop_graph()
                benchmarker.create_schema(is_property_graph=False)
                benchmarker.load_data(data_path, undirected)

            elif command == CommandType.test_basic_op:
                benchmarker.basic_op_test()

            elif command == CommandType.test_read_write_op:
                benchmarker.read_write_test(read_ratio)
                
            elif command == CommandType.test_algm:
                benchmarker.test_algm(algm)

    except Exception as e:
        typer.echo(f"\nAn error occurred: {e}", err=True)
        # import traceback
        # traceback.print_exc() # Uncomment for detailed debugging
        raise typer.Exit(code=1)
    
if __name__ == "__main__":
    app()