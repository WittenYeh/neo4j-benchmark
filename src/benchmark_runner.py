# file: benchmark_runner.py

import time
import random
import platform
import sys
import statistics
from pathlib import Path
from neo4j import GraphDatabase
from rich.progress import track
from rich import print as rprint

class BenchmarkRunner:
    """Manages the execution of Neo4j benchmarks, including reporting and statistics."""

    # Inner class for detailed latency statistics
    class _LatencyStats:
        def __init__(self, timings_ns: list[int]):
            if not timings_ns:
                self.count = 0
                return

            self.count = len(timings_ns)
            timings_us = sorted([t / 1000.0 for t in timings_ns])

            self.avg_us = statistics.mean(timings_us)
            self.min_us = timings_us[0]
            self.max_us = timings_us[-1]
            self.p50_us = statistics.median(timings_us)
            # Use a simple indexing method for percentiles, safe for empty lists
            self.p95_us = timings_us[int(self.count * 0.95)] if self.count > 20 else self.p50_us
            self.p99_us = timings_us[int(self.count * 0.99)] if self.count > 100 else self.max_us

        def __str__(self):
            if self.count == 0:
                return "Latency: N/A (no successful operations recorded)"
            return (
                f"Avg Latency: {self.avg_us:,.2f} us\n"
                f"Min Latency: {self.min_us:,.2f} us\n"
                f"Max Latency: {self.max_us:,.2f} us\n"
                f"p50 Latency: {self.p50_us:,.2f} us\n"
                f"p95 Latency: {self.p95_us:,.2f} us\n"
                f"p99 Latency: {self.p99_us:,.2f} us"
            )

    # Inner class for managing report output
    class _ReportManager:
        def __init__(self, report_path: str | None):
            self.report_path = Path(report_path) if report_path else None
            self._report_lines = []

        def append(self, message: str, *args, **kwargs):
            # The 'console_only' flag prevents writing a line to the report file
            console_only = kwargs.get('console_only', False)
            
            formatted_message = message.format(*args)
            rprint(formatted_message)
            if self.report_path and not console_only:
                self._report_lines.append(formatted_message)

        def append_section_header(self, title: str):
            header = f"\n{'='*20} {title.upper()} {'='*20}"
            self.append(header)

        def write_to_file(self):
            if self.report_path and self._report_lines:
                try:
                    # Ensure the final "End of Report" is only in the file, not console
                    self._report_lines.append("\n--- End of Report ---")
                    self.report_path.write_text("\n".join(self._report_lines), encoding='utf-8')
                    self.append(f"\n[bold green]Benchmark report saved to: {self.report_path}[/bold green]", console_only=True)
                except IOError as e:
                    self.append(f"[bold red]Error: Failed to write report to {self.report_path}: {e}[/bold red]", console_only=True)

    def __init__(self, uri, user, password, graph_name, report_path=None):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.graph_name = graph_name
        self.report_manager = self._ReportManager(report_path)
        self.BATCH_SIZE = 2048
        self.MAX_OPS = 20000
        self.MAX_BASIC_OP = 1000
        if report_path: # Only add header if we are actually generating a report
             self._add_report_header()

    def _add_report_header(self):
        self.report_manager.append("="*60)
        self.report_manager.append("        Neo4j Python Benchmark Report")
        self.report_manager.append("="*60)
        self.report_manager.append(f"Date: {time.strftime('%Y-%m-%d %H:%M:%S')}")
        self.report_manager.append(f"Graph Name: {self.graph_name}")
        self.report_manager.append(f"OS: {platform.system()} {platform.release()}")
        self.report_manager.append(f"Python Version: {sys.version.split()[0]}")
        self.report_manager.append("-"*60)

    def close(self):
        self.report_manager.write_to_file()
        self.driver.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def drop_graph(self):
        self.report_manager.append_section_header("Dropping Graph")
        try:
            self.driver.execute_query("CALL gds.graph.drop($graphName, false)", graphName=self.graph_name, database_="neo4j")
            self.report_manager.append("GDS graph projection dropped (if existed).")
        except Exception:
            self.report_manager.append("Could not drop GDS projection (may not exist).")

        property_v_label = f"{self.graph_name}_PropertyV"
        self.driver.execute_query(f"DROP INDEX {self.graph_name}_id_index IF EXISTS", database_="neo4j")
        self.driver.execute_query(f"DROP INDEX {property_v_label}_id_index IF EXISTS", database_="neo4j")
        self.report_manager.append("Indexes dropped (if existed).")

        self.report_manager.append("Deleting graph data...")
        delete_query = "MATCH (n) WHERE labels(n)[0] STARTS WITH $graphName CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS"
        self.driver.execute_query(delete_query, graphName=self.graph_name, database_="neo4j")
        self.report_manager.append("Graph data deleted.")

    def create_schema(self, is_property_graph: bool):
        self.report_manager.append_section_header("Creating Schema")
        node_label = f"{self.graph_name}_PropertyV" if is_property_graph else self.graph_name
        self.driver.execute_query(f"CREATE INDEX {node_label}_id_index IF NOT EXISTS FOR (n:{node_label}) ON (n.id)", database_="neo4j")
        self.report_manager.append(f"Index on {node_label}(id) created.")
        self.report_manager.append("Waiting 10 seconds for index to be online...")
        time.sleep(10)

    def load_data(self, data_path: str, is_undirected: bool):
        self.report_manager.append_section_header(f"Loading Graph Data from {data_path}")
        start_load_time = time.time()
        nodes, edges = set(), []

        self.report_manager.append("Reading data file...", console_only=True)
        with open(data_path, 'r') as f:
            for line in f:
                parts = line.strip().split()
                if len(parts) < 2: continue
                try:
                    src, dst = int(parts[0]), int(parts[1])
                    nodes.add(src)
                    nodes.add(dst)
                    edges.append({"src": src, "dst": dst})
                    if is_undirected:
                        edges.append({"src": dst, "dst": src})
                except ValueError: pass
        self.report_manager.append(f"Finished reading file. Found {len(nodes):,} unique nodes and {len(edges):,} edges.")

        node_list = [{"id": n} for n in nodes]
        with self.driver.session(database="neo4j") as session:
            for i in track(range(0, len(node_list), self.BATCH_SIZE), description="[green]Inserting nodes..."):
                batch = node_list[i:i + self.BATCH_SIZE]
                session.run(f"UNWIND $nodes AS data MERGE (n:{self.graph_name} {{id: data.id}})", nodes=batch)
            self.report_manager.append(f"Inserted/Merged {len(node_list):,} nodes.")

            for i in track(range(0, len(edges), self.BATCH_SIZE), description="[cyan]Inserting relationships..."):
                batch = edges[i:i + self.BATCH_SIZE]
                session.run(f"UNWIND $edges AS edge MATCH (s:{self.graph_name} {{id: edge.src}}), (d:{self.graph_name} {{id: edge.dst}}) MERGE (s)-[:REL]->(d)", edges=batch)
            self.report_manager.append(f"Inserted/Merged {len(edges):,} relationships.")
        
        duration = time.time() - start_load_time
        self.report_manager.append("\n--- Data Loading Summary ---")
        self.report_manager.append(f"Total time: {duration:.3f} seconds")
        if duration > 0:
            self.report_manager.append(f"Node loading throughput: {len(nodes) / duration:,.2f} nodes/sec")
            self.report_manager.append(f"Edge loading throughput: {len(edges) / duration:,.2f} edges/sec")

    def _get_max_vid(self, node_label: str) -> int:
        records, _, _ = self.driver.execute_query(f"MATCH (n:{node_label}) RETURN max(n.id) AS maxId", database_="neo4j")
        return records[0]["maxId"] if records and records[0]["maxId"] is not None else 0

    def basic_op_test(self):
        self.report_manager.append_section_header("Basic Operations Latency Test")
        node_label = self.graph_name
        max_vid = self._get_max_vid(node_label)
        if max_vid <= 1:
            self.report_manager.append("[bold red]ERROR: Max VID is too small. Cannot run basic op test.[/bold red]")
            return

        with self.driver.session(database="neo4j") as session:
            # 1. Add Vertex
            latencies_ns = []
            for i in track(range(self.MAX_BASIC_OP), description="[yellow]Add Vertex..."):
                vid = max_vid + i + 1
                start = time.perf_counter_ns()
                session.run(f"CREATE (n:{node_label} {{id: $id}})", id=vid)
                latencies_ns.append(time.perf_counter_ns() - start)
            self.report_manager.append("\n--- Operation: Add Vertex ---")
            self.report_manager.append(f"Total Operations: {self.MAX_BASIC_OP}")
            self.report_manager.append(str(self._LatencyStats(latencies_ns)))

            # 2. Add Edge
            latencies_ns = []
            for _ in track(range(self.MAX_BASIC_OP), description="[yellow]Add Edge..."):
                src, dst = random.randint(1, max_vid), random.randint(1, max_vid)
                start = time.perf_counter_ns()
                session.run(f"MATCH (a:{node_label} {{id: $src}}), (b:{node_label} {{id: $dst}}) CREATE (a)-[:REL]->(b)", src=src, dst=dst)
                latencies_ns.append(time.perf_counter_ns() - start)
            self.report_manager.append("\n--- Operation: Add Edge ---")
            self.report_manager.append(f"Total Operations: {self.MAX_BASIC_OP}")
            self.report_manager.append(str(self._LatencyStats(latencies_ns)))

            # 3. Get Neighbors
            latencies_ns = []
            for _ in track(range(self.MAX_BASIC_OP), description="[yellow]Get Neighbors..."):
                vid = random.randint(1, max_vid)
                start = time.perf_counter_ns()
                session.run(f"MATCH (a:{node_label} {{id: $vid}})-->(b) RETURN b.id", vid=vid).consume()
                latencies_ns.append(time.perf_counter_ns() - start)
            self.report_manager.append("\n--- Operation: Get Neighbors ---")
            self.report_manager.append(f"Total Operations: {self.MAX_BASIC_OP}")
            self.report_manager.append(str(self._LatencyStats(latencies_ns)))

    def read_write_test(self, read_ratio: float):
        self.report_manager.append_section_header("Read/Write Throughput Test")
        self.report_manager.append(f"Configuration: Total Ops={self.MAX_OPS:,}, Read Ratio={read_ratio:.2f}")
        node_label = self.graph_name
        max_vid = self._get_max_vid(node_label)
        if max_vid <= 1:
            self.report_manager.append("[bold red]ERROR: Max VID is too small. Cannot run test.[/bold red]")
            return

        read_ops = int(self.MAX_OPS * read_ratio)
        ops = [0] * read_ops + [1] * (self.MAX_OPS - read_ops)
        random.shuffle(ops)

        start_time = time.perf_counter_ns()
        with self.driver.session(database="neo4j") as session:
            for op in track(ops, description=f"[magenta]R/W Test (Ratio {read_ratio})..."):
                if op == 0: # Read
                    vid = random.randint(1, max_vid)
                    session.run(f"MATCH (a:{node_label} {{id: $vid}})-->(b) RETURN b.id", vid=vid).consume()
                else: # Write
                    src, dst = random.randint(1, max_vid), random.randint(1, max_vid)
                    session.run(f"MATCH (a:{node_label} {{id: $src}}), (b:{node_label} {{id: $dst}}) CREATE (a)-[:REL]->(b)", src=src, dst=dst)
        duration_ns = time.perf_counter_ns() - start_time
        
        seconds = duration_ns / 1_000_000_000.0
        self.report_manager.append("\n--- Throughput Test Summary ---")
        self.report_manager.append(f"Total operations executed: {self.MAX_OPS:,}")
        self.report_manager.append(f"Total time: {seconds:.3f} seconds")
        if seconds > 0:
            self.report_manager.append(f"Throughput: {self.MAX_OPS / seconds:,.2f} op/s")

    def test_algm(self, algm: str):
        self.report_manager.append_section_header(f"GDS Algorithm Test (algm: {algm})")
        node_label, rel_type = self.graph_name, "REL"

        with self.driver.session(database="neo4j") as session:
            self.report_manager.append("Projecting graph to GDS...")
            session.run("CALL gds.graph.project($g, $n, $r)", g=self.graph_name, n=node_label, r=rel_type)

            algos_to_run = {
                "pagerank": "CALL gds.pageRank.stream($g) YIELD score RETURN count(*)",
                "wcc": "CALL gds.wcc.stream($g) YIELD componentId RETURN count(*)",
                "cdlp": "CALL gds.labelPropagation.stream($g) YIELD communityId RETURN count(*)",
                "sssp": f"MATCH (s:{node_label} {{id:1}}) CALL gds.sssp.stream($g, {{sourceNode:s}}) YIELD distance RETURN count(*)",
                "bfs": f"MATCH (s:{node_label} {{id:1}}) CALL gds.bfs.stream($g, {{sourceNode:s}}) YIELD path RETURN count(*)"
            }

            for name, query in algos_to_run.items():
                if algm in ("all", name):
                    start = time.time()
                    session.run(query, g=self.graph_name).consume()
                    self.report_manager.append(f"{name.upper()} Done, Time: {time.time() - start:.3f} s")
            
            self.report_manager.append("Cleaning up GDS projection...")
            session.run("CALL gds.graph.drop($g, false)", g=self.graph_name)