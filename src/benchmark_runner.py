import time
import random
from neo4j import GraphDatabase
from rich.progress import track, Progress

class BenchmarkRunner:
    def __init__(self, uri, user, password, graph_name):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.graph_name = graph_name
        self.BATCH_SIZE = 2048
        self.MAX_OPS = 20000  # Total operations for throughput test
        self.MAX_BASIC_OP = 1000  # Operations per latency test

    def close(self):
        self.driver.close()

    # Context manager support for 'with' statement
    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def drop_graph(self):
        print(f"Dropping graph '{self.graph_name}'...")
        try:
            with self.driver.session() as session:
                session.run("CALL gds.graph.drop($graphName, false) YIELD graphName", graphName=self.graph_name)
                print("GDS graph projection dropped (if existed).")
        except Exception:
            print("Could not drop GDS projection (may not exist).")

        with self.driver.session() as session:
            property_v_label = f"{self.graph_name}_PropertyV"
            session.run(f"DROP INDEX {self.graph_name}_id_index IF EXISTS")
            session.run(f"DROP INDEX {property_v_label}_id_index IF EXISTS")
            print("Indexes dropped (if existed).")

            print("Deleting graph data...")
            delete_query = f"""
            MATCH (n) WHERE labels(n)[0] STARTS WITH $graphName
            CALL {{ WITH n DETACH DELETE n }} IN TRANSACTIONS OF 10000 ROWS
            """
            session.run(delete_query, graphName=self.graph_name)
            print("Graph data deleted.")

    def create_schema(self, is_property_graph: bool):
        print("Creating schema (indexes)...")
        node_label = f"{self.graph_name}_PropertyV" if is_property_graph else self.graph_name
        with self.driver.session() as session:
            session.run(f"CREATE INDEX {node_label}_id_index IF NOT EXISTS FOR (n:{node_label}) ON (n.id)")
            print(f"Index on {node_label}(id) created.")
        
        print("Waiting 10 seconds for index to be online...")
        time.sleep(10)

    def load_data(self, data_path: str, is_undirected: bool):
        print(f"Loading data from '{data_path}'...")
        nodes = set()
        edges = []

        print("Reading data file...")
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
                except ValueError:
                    # Ignore non-numeric lines
                    pass
        print(f"Finished reading file. Found {len(nodes)} unique nodes and {len(edges)} edges.")

        node_list = [{"id": n} for n in nodes]
        
        with self.driver.session() as session:
            # Batch insert nodes
            print(f"Loading {len(node_list)} nodes...")
            for i in track(range(0, len(node_list), self.BATCH_SIZE), description="Inserting nodes..."):
                batch = node_list[i:i + self.BATCH_SIZE]
                session.run(f"UNWIND $nodes AS node_data MERGE (n:{self.graph_name} {{id: node_data.id}})", nodes=batch)
            print(f"Inserted/Merged {len(node_list)} nodes.")

            # Batch insert relationships
            print(f"Loading {len(edges)} relationships...")
            for i in track(range(0, len(edges), self.BATCH_SIZE), description="Inserting relationships..."):
                batch = edges[i:i + self.BATCH_SIZE]
                session.run(f"""
                    UNWIND $edges AS edge
                    MATCH (src:{self.graph_name} {{id: edge.src}})
                    MATCH (dst:{self.graph_name} {{id: edge.dst}})
                    MERGE (src)-[:REL]->(dst)
                """, edges=batch)
            print(f"Inserted/Merged {len(edges)} relationships.")

    def load_property_graph(self, data_path: str):
        vertex_label = f"{self.graph_name}_PropertyV"
        edge_type = f"{self.graph_name}_PropertyE"
        vertex_file = f"{data_path}.vertex"
        edge_file = f"{data_path}.edge"

        with self.driver.session() as session:
            # Load vertices
            print(f"Loading vertices from {vertex_file}")
            vertices = []
            with open(vertex_file, 'r') as f:
                for line in f:
                    if not line.strip(): continue
                    parts = line.strip().split()
                    props = {"id": int(parts[0])}
                    for part in parts[1:]:
                        kv = part.split(':', 1)
                        if len(kv) == 2: props[kv[0]] = kv[1]
                    vertices.append(props)
            
            for i in track(range(0, len(vertices), self.BATCH_SIZE), description="Inserting vertices..."):
                batch = vertices[i:i + self.BATCH_SIZE]
                session.run(f"UNWIND $batch as props CREATE (n:{vertex_label}) SET n = props", batch=batch)
            print(f"Loaded {len(vertices)} vertices.")

            # Load edges
            print(f"Loading edges from {edge_file}")
            edges = []
            with open(edge_file, 'r') as f:
                for line in f:
                    if not line.strip(): continue
                    parts = line.strip().split()
                    edge_data = {"src": int(parts[0]), "dst": int(parts[1]), "props": {}}
                    for part in parts[2:]:
                        kv = part.split(':', 1)
                        if len(kv) == 2: edge_data["props"][kv[0]] = kv[1]
                    edges.append(edge_data)

            for i in track(range(0, len(edges), self.BATCH_SIZE), description="Inserting edges..."):
                batch = edges[i:i + self.BATCH_SIZE]
                session.run(f"""
                    UNWIND $batch as edge
                    MATCH (src:{vertex_label} {{id: edge.src}})
                    MATCH (dst:{vertex_label} {{id: edge.dst}})
                    CREATE (src)-[r:{edge_type}]->(dst) SET r = edge.props
                """, batch=batch)
            print(f"Loaded {len(edges)} edges.")

    def _get_max_vid(self, node_label: str) -> int:
        with self.driver.session() as session:
            result = session.run(f"MATCH (n:{node_label}) RETURN max(n.id) AS maxId")
            record = result.single()
            return record["maxId"] if record and record["maxId"] is not None else 0
    
    def basic_op_test(self):
        print("Running basic operations latency test...")
        node_label = self.graph_name
        max_vid = self._get_max_vid(node_label)
        if max_vid <= 1:
            print("Max VID is too small. Cannot run basic op test. Is the graph loaded correctly?")
            return

        with self.driver.session() as session:
            # 1. Add Vertex
            duration_ns = 0
            for _ in track(range(self.MAX_BASIC_OP), description="Add Vertex..."):
                vid = random.randint(max_vid + 1, max_vid + self.MAX_BASIC_OP + 1)
                start = time.perf_counter_ns()
                session.run(f"CREATE (n:{node_label} {{id: $id}})", id=vid)
                end = time.perf_counter_ns()
                duration_ns += end - start
            print(f"Add Vertex Latency: {duration_ns / self.MAX_BASIC_OP / 1000.0:.2f} us")

            # 2. Add Edge
            duration_ns = 0
            for _ in track(range(self.MAX_BASIC_OP), description="Add Edge..."):
                src, dst = random.randint(1, max_vid -1), random.randint(1, max_vid -1)
                start = time.perf_counter_ns()
                session.run(f"MATCH (a:{node_label} {{id: $src}}), (b:{node_label} {{id: $dst}}) CREATE (a)-[:REL]->(b)", src=src, dst=dst)
                end = time.perf_counter_ns()
                duration_ns += end - start
            print(f"Add Edge Latency: {duration_ns / self.MAX_BASIC_OP / 1000.0:.2f} us")

            # 3. Delete Edge
            duration_ns = 0
            successful_deletions = 0
            for _ in track(range(self.MAX_BASIC_OP), description="Delete Edge..."):
                src = random.randint(1, max_vid - 1)
                # Find a neighbor first (not timed)
                find_neighbor_result = session.run(f"MATCH (a:{node_label} {{id: $src}})-[:REL]->(b) RETURN b.id AS dstId LIMIT 1", src=src)
                record = find_neighbor_result.single()
                if record:
                    dst_id = record["dstId"]
                    # Time only the deletion
                    start = time.perf_counter_ns()
                    session.run(f"MATCH (a:{node_label} {{id: $src}})-[r:REL]->(b:{node_label} {{id: $dst}}) WITH r LIMIT 1 DELETE r", src=src, dst=dst_id)
                    end = time.perf_counter_ns()
                    duration_ns += (end - start)
                    successful_deletions += 1
            if successful_deletions > 0:
                 print(f"Delete Edge Latency: {duration_ns / successful_deletions / 1000.0:.2f} us (based on {successful_deletions} successful deletions)")
            else:
                print("Delete Edge Latency: N/A (no edges were found to delete)")
                
            # 4. Get Neighbors
            duration_ns = 0
            for _ in track(range(self.MAX_BASIC_OP), description="Get Neighbors..."):
                vid = random.randint(1, max_vid - 1)
                start = time.perf_counter_ns()
                # Use consume() to ensure we process the full result from the server
                session.run(f"MATCH (a:{node_label} {{id: $vid}})-->(b) RETURN b.id", vid=vid).consume()
                end = time.perf_counter_ns()
                duration_ns += (end - start)
            print(f"Get Neighbors Latency: {duration_ns / self.MAX_BASIC_OP / 1000.0:.2f} us")

    def read_write_test(self, read_ratio: float):
        print(f"Running throughput test with read ratio {read_ratio:.2f}...")
        node_label = self.graph_name
        max_vid = self._get_max_vid(node_label)
        if max_vid <= 1:
            print("Max VID is too small. Cannot run throughput test.")
            return

        print("Generating and shuffling operation list...")
        ops = []
        read_ops_count = int(self.MAX_OPS * read_ratio)
        write_ops_count = self.MAX_OPS - read_ops_count
        ops.extend([0] * read_ops_count) # 0 for read
        ops.extend([1] * write_ops_count) # 1 for write
        random.shuffle(ops)
        print("Operation list generated.")

        duration_ns = 0
        with self.driver.session() as session:
            for op in track(ops, description="Read/Write Test..."):
                if op == 0: # Read
                    vid = random.randint(1, max_vid - 1)
                    start = time.perf_counter_ns()
                    session.run(f"MATCH (a:{node_label} {{id: $vid}})-->(b) RETURN b.id", vid=vid).consume()
                    end = time.perf_counter_ns()
                    duration_ns += end - start
                else: # Write (Add Edge)
                    src, dst = random.randint(1, max_vid - 1), random.randint(1, max_vid - 1)
                    start = time.perf_counter_ns()
                    session.run(f"MATCH (a:{node_label} {{id: $src}}), (b:{node_label} {{id: $dst}}) CREATE (a)-[:REL]->(b)", src=src, dst=dst)
                    end = time.perf_counter_ns()
                    duration_ns += end - start
        
        total_seconds = duration_ns / 1_000_000_000.0
        print(f"Read Write Test Done, Time: {total_seconds * 1000:.2f} ms")
        if total_seconds > 0:
            throughput = self.MAX_OPS / total_seconds
            print(f"Throughput: {throughput:.2f} op/s")
        else:
            print("Throughput: N/A (duration too short to measure)")

    def property_op_test(self):
        print("Running property operations latency test...")
        node_label = f"{self.graph_name}_PropertyV"
        max_vid = self._get_max_vid(node_label)
        if max_vid <= 1:
            print("Max VID is too small. Cannot run property test. Is the property graph loaded?")
            return
        
        with self.driver.session() as session:
            # Add Vertex Property
            duration_ns = 0
            for i in track(range(self.MAX_BASIC_OP), description="Add Property..."):
                vid = random.randint(1, max_vid - 1)
                start = time.perf_counter_ns()
                session.run(f"MATCH (n:{node_label} {{id: $id}}) SET n.newProp = $val", id=vid, val=i)
                end = time.perf_counter_ns()
                duration_ns += end - start
            print(f"Add Vertex Property Latency: {duration_ns / self.MAX_BASIC_OP / 1000.0:.2f} us")

            # Delete Vertex Property
            duration_ns = 0
            for _ in track(range(self.MAX_BASIC_OP), description="Delete Property..."):
                vid = random.randint(1, max_vid - 1)
                start = time.perf_counter_ns()
                session.run(f"MATCH (n:{node_label} {{id: $id}}) REMOVE n.newProp", id=vid)
                end = time.perf_counter_ns()
                duration_ns += end - start
            print(f"Delete Vertex Property Latency: {duration_ns / self.MAX_BASIC_OP / 1000.0:.2f} us")

    def test_algm(self, algm: str):
        print(f"Testing algorithm(s): {algm}")
        node_label = self.graph_name
        rel_type = "REL"

        with self.driver.session() as session:
            # Step 1: Project graph into GDS
            print("Projecting graph to GDS...")
            session.run("CALL gds.graph.project($graphName, $nodeLabel, $relType)",
                        graphName=self.graph_name, nodeLabel=node_label, relType=rel_type)
            
            # Step 2: Run algorithms
            if algm in ("all", "pagerank"):
                start = time.time()
                session.run("CALL gds.pageRank.stream($graphName) YIELD nodeId, score RETURN count(*)", graphName=self.graph_name).consume()
                print(f"PageRank Done, Time: {time.time() - start:.3f} s")
            
            if algm in ("all", "wcc"):
                start = time.time()
                session.run("CALL gds.wcc.stream($graphName) YIELD nodeId, componentId RETURN count(*)", graphName=self.graph_name).consume()
                print(f"WCC Done, Time: {time.time() - start:.3f} s")

            if algm in ("all", "cdlp"):
                start = time.time()
                session.run("CALL gds.labelPropagation.stream($graphName) YIELD nodeId, communityId RETURN count(*)", graphName=self.graph_name).consume()
                print(f"CDLP Done, Time: {time.time() - start:.3f} s")

            if algm in ("all", "sssp"):
                start_vid = 1
                start = time.time()
                session.run(f"""
                    MATCH (start:{node_label} {{id: $startId}})
                    CALL gds.sssp.stream($graphName, {{ sourceNode: start }}) YIELD nodeId, distance RETURN count(*)
                """, graphName=self.graph_name, startId=start_vid).consume()
                print(f"SSSP Done, Time: {time.time() - start:.3f} s")
                
            if algm in ("all", "bfs"):
                start_vid = 1
                start = time.time()
                session.run(f"""
                    MATCH (start:{node_label} {{id: $startId}})
                    CALL gds.bfs.stream($graphName, {{ sourceNode: start }}) YIELD path RETURN count(*)
                """, graphName=self.graph_name, startId=start_vid).consume()
                print(f"BFS Done, Time: {time.time() - start:.3f} s")

            # Clean up GDS projection
            session.run("CALL gds.graph.drop($graphName, false)", graphName=self.graph_name)