import org.neo4j.driver.*;
import org.neo4j.driver.Record;
import org.neo4j.driver.exceptions.Neo4jException;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@Command(name = "neo4j-benchmark", mixinStandardHelpOptions = true,
        description = "A benchmark tool for Neo4j, mirroring the Nebula Graph test suite.")
public class Neo4jBenchmark implements Callable<Integer> {

    // --- Command Line Arguments ---
    @Option(names = "--uri", description = "Neo4j Bolt URI", defaultValue = "bolt://localhost:7687")
    private String uri;

    @Option(names = "--user", description = "Neo4j username", defaultValue = "neo4j")
    private String user;

    @Option(names = "--password", description = "Neo4j password", defaultValue = "password")
    private String password;

    @Option(names = "--graph-name", required = true, description = "The name of the graph to be tested.")
    private String graphName;

    @Option(names = "--data-path", description = "The path of the data file to be loaded.")
    private String dataPath;

    @Option(names = "--command", required = true, description = "The command to execute: ${COMPLETION-CANDIDATES}")
    private CommandType command;

    @Option(names = "--undirected", description = "Whether the graph is undirected.")
    private boolean undirected;

    @Option(names = "--read-ratio", description = "The ratio of read/lookup operations in the throughput test.", defaultValue = "0.5")
    private double readRatio;

    @Option(names = "--algm", description = "The algorithm to be tested.", defaultValue = "all")
    private String algm;

    private enum CommandType { load_graph, load_property, test_read_write_op, test_basic_op, test_property, test_algm }

    // --- Benchmarker Class ---
    private static class Neo4jBenchmarker implements AutoCloseable {
        private final Driver driver;
        private final String graphName;
        private final int BATCH_SIZE = 512;
        private final long MAX_OPS = 200000; // Total operations for throughput test
        private final int MAX_BASIC_OP = 10000; // Operations per latency test

        // Define the interval for progress updates (e.g., update every 50 batches)
        private final int PROGRESS_BATCH_INTERVAL = 50;

        public Neo4jBenchmarker(String uri, String user, String password, String graphName) {
            this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
            this.graphName = graphName;
        }

        @Override
        public void close() {
            driver.close();
        }

        // --- Schema and Data Management ---

        public void dropGraph() {
            System.out.println("Dropping graph '" + graphName + "'...");
            try (Session session = driver.session()) {
                // Drop GDS projection if it exists
                session.run("CALL gds.graph.drop($graphName, false) YIELD graphName", Map.of("graphName", graphName));
                System.out.println("GDS graph projection dropped (if existed).");
            } catch (Exception e) {
                System.out.println("Could not drop GDS projection (may not exist).");
            }

            try (Session session = driver.session()) {
                // Drop indexes
                String propertyVLabel = graphName + "_PropertyV";
                session.run("DROP INDEX " + graphName + "_id_index IF EXISTS");
                session.run("DROP INDEX " + propertyVLabel + "_id_index IF EXISTS");
                System.out.println("Indexes dropped (if existed).");
                
                // Drop data in batches for better memory management
                System.out.println("Deleting graph data...");
                String deleteQuery = "MATCH (n) WHERE labels(n)[0] STARTS WITH $graphName CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS";
                session.run(deleteQuery, Map.of("graphName", graphName));
                System.out.println("Graph data deleted.");
            }
        }

        public void createSchema(boolean isPropertyGraph) {
            System.out.println("Creating schema (indexes)...");
            String nodeLabel = isPropertyGraph ? graphName + "_PropertyV" : graphName;
            try (Session session = driver.session()) {
                session.run("CREATE INDEX " + nodeLabel + "_id_index IF NOT EXISTS FOR (n:" + nodeLabel + ") ON (n.id)");
                System.out.println("Index on " + nodeLabel + "(id) created.");
            }
            // Wait for index to come online for better performance on initial load
            System.out.println("Waiting 10 seconds for index to be online...");
            try { TimeUnit.SECONDS.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }

        public void loadData(String dataPath, boolean isUndirected) throws IOException {
            System.out.println("Loading data from '" + dataPath + "'...");
            Set<Long> nodes = new HashSet<>();
            List<Map<String, Object>> edges = new ArrayList<>();

            long lineCounter = 0;
            final int PROGRESS_INTERVAL = 100000; // Update progress every 100,000 lines
            System.out.println("Reading data file...");

            try (BufferedReader reader = new BufferedReader(new FileReader(dataPath))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    lineCounter++;
                    if (lineCounter % PROGRESS_INTERVAL == 0) {
                        System.out.printf("\rProcessed %d lines...", lineCounter);
                    }
                    String[] parts = line.trim().split("\\s+");
                    if (parts.length < 2) continue;
                    try {
                    long src = Long.parseLong(parts[0]);
                    long dst = Long.parseLong(parts[1]);
                    nodes.add(src);
                    nodes.add(dst);
                    edges.add(Map.of("src", src, "dst", dst));
                    if (isUndirected) {
                        edges.add(Map.of("src", dst, "dst", src));
                    }
                    } catch (NumberFormatException e) {
                        // Ignore lines that are not valid numbers
                    }
                }
            }
            System.out.printf("\rProcessed %d lines... Done.\n", lineCounter);

            List<Map<String, Object>> nodeList = nodes.stream().map(n -> Map.of("id", (Object)n)).collect(Collectors.toList());

            try (Session session = driver.session()) {
                // Batch insert nodes
                System.out.printf("Loading %d nodes...\n", nodes.size());
                int batchCounter = 0;
                for (int i = 0; i < nodeList.size(); i += BATCH_SIZE) {
                    List<Map<String, Object>> batch = nodeList.subList(i, Math.min(i + BATCH_SIZE, nodeList.size()));
                    session.run("UNWIND $nodes AS node_data MERGE (n:" + graphName + " {id: node_data.id})", Map.of("nodes", batch));
                    
                    batchCounter++;
                    int processed = Math.min(i + BATCH_SIZE, nodeList.size());
                    // Update progress every N batches or on the very last batch
                    if (batchCounter % PROGRESS_BATCH_INTERVAL == 0 || processed == nodeList.size()) {
                    double percentage = nodeList.isEmpty() ? 100.0 : (double) processed / nodeList.size() * 100;
                    System.out.printf("\rInserted nodes: %d / %d (%.2f%%)", processed, nodeList.size(), percentage);
                    }
                }
                System.out.println();
                System.out.printf("Inserted/Merged %d nodes.\n", nodes.size());

                // Batch insert relationships
                System.out.printf("Loading %d relationships...\n", edges.size());
                batchCounter = 0; // Reset batch counter
                for (int i = 0; i < edges.size(); i += BATCH_SIZE) {
                    List<Map<String, Object>> batch = edges.subList(i, Math.min(i + BATCH_SIZE, edges.size()));
                    session.run("UNWIND $edges AS edge " +
                                "MATCH (src:" + graphName + " {id: edge.src}) " +
                                "MATCH (dst:" + graphName + " {id: edge.dst}) " +
                                "MERGE (src)-[:REL]->(dst)", Map.of("edges", batch));

                    batchCounter++;
                    int processed = Math.min(i + BATCH_SIZE, edges.size());
                    // Update progress every N batches or on the very last batch
                    if (batchCounter % PROGRESS_BATCH_INTERVAL == 0 || processed == edges.size()) {
                    double percentage = edges.isEmpty() ? 100.0 : (double) processed / edges.size() * 100;
                    System.out.printf("\rInserted relationships: %d / %d (%.2f%%)", processed, edges.size(), percentage);
                    }
                }
                System.out.println();
                System.out.printf("Inserted/Merged %d relationships.\n", edges.size());
            }
        }

        public void loadPropertyGraph(String dataPath) throws IOException {
            String vertexLabel = graphName + "_PropertyV";
            String edgeType = graphName + "_PropertyE";
            Path vertexFile = Paths.get(dataPath + ".vertex");
            Path edgeFile = Paths.get(dataPath + ".edge");

            if (!Files.exists(vertexFile) || !Files.exists(edgeFile)) {
                throw new IOException("Data files not found! Expected: " + vertexFile + " and " + edgeFile);
            }

            try(Session session = driver.session()) {
                // Load Vertices
                System.out.println("Loading vertices from " + vertexFile);
                List<Map<String, Object>> vertices = new ArrayList<>();
                for(String line : Files.readAllLines(vertexFile)) {
                     if (line.trim().isEmpty()) continue;
                     String[] parts = line.trim().split("\\s+");
                     Map<String, Object> props = new HashMap<>();
                     props.put("id", Long.parseLong(parts[0]));
                     for(int i = 1; i < parts.length; i++) {
                         String[] kv = parts[i].split(":", 2);
                         if (kv.length == 2) props.put(kv[0], kv[1]);
                     }
                     vertices.add(props);
                }
                 int batchCounter = 0;
                 for (int i = 0; i < vertices.size(); i += BATCH_SIZE) {
                     List<Map<String, Object>> batch = vertices.subList(i, Math.min(i + BATCH_SIZE, vertices.size()));
                     session.run("UNWIND $batch as props CREATE (n:" + vertexLabel + ") SET n = props", Map.of("batch", batch));
                     
                     batchCounter++;
                     int processed = Math.min(i + BATCH_SIZE, vertices.size());
                     if (batchCounter % PROGRESS_BATCH_INTERVAL == 0 || processed == vertices.size()) {
                        double percentage = vertices.isEmpty() ? 100.0 : (double) processed / vertices.size() * 100;
                        System.out.printf("\rInserting vertices: %d / %d (%.2f%%)", processed, vertices.size(), percentage);
                     }
                 }
                 System.out.println();
                System.out.printf("Loaded %d vertices.\n", vertices.size());

                // Load Edges
                System.out.println("Loading edges from " + edgeFile);
                List<Map<String, Object>> edges = new ArrayList<>();
                 for(String line : Files.readAllLines(edgeFile)) {
                     if (line.trim().isEmpty()) continue;
                     String[] parts = line.trim().split("\\s+");
                     Map<String, Object> edgeData = new HashMap<>();
                     edgeData.put("src", Long.parseLong(parts[0]));
                     edgeData.put("dst", Long.parseLong(parts[1]));
                     Map<String, Object> props = new HashMap<>();
                     for(int i = 2; i < parts.length; i++) {
                          String[] kv = parts[i].split(":", 2);
                          if (kv.length == 2) props.put(kv[0], kv[1]);
                     }
                     edgeData.put("props", props);
                     edges.add(edgeData);
                 }
                 batchCounter = 0;
                 for (int i = 0; i < edges.size(); i += BATCH_SIZE) {
                     List<Map<String, Object>> batch = edges.subList(i, Math.min(i + BATCH_SIZE, edges.size()));
                     session.run("UNWIND $batch as edge " +
                                 "MATCH (src:" + vertexLabel + " {id: edge.src}) " +
                                 "MATCH (dst:" + vertexLabel + " {id: edge.dst}) " +
                                 "CREATE (src)-[r:" + edgeType + "]->(dst) SET r = edge.props", Map.of("batch", batch));
                     batchCounter++;
                     int processed = Math.min(i + BATCH_SIZE, edges.size());
                     if (batchCounter % PROGRESS_BATCH_INTERVAL == 0 || processed == edges.size()) {
                        double percentage = edges.isEmpty() ? 100.0 : (double) processed / edges.size() * 100;
                        System.out.printf("\rInserting edges: %d / %d (%.2f%%)", processed, edges.size(), percentage);
                     }
                 }
                 System.out.println();
                System.out.printf("Loaded %d edges.\n", edges.size());
            }
        }

        private long getMaxVid(String nodeLabel) {
            try (Session session = driver.session()) {
                Result result = session.run("MATCH (n:" + nodeLabel + ") RETURN max(n.id) AS maxId");
                if (result.hasNext()) {
                    Value maxIdValue = result.single().get("maxId");
                    if (!maxIdValue.isNull()) {
                        return maxIdValue.asLong();
                    }
                }
                return 0L;
            }
        }
        
        // --- Test Execution ---

        public void basicOpTest() {
            System.out.println("Running basic operations latency test...");
            String nodeLabel = graphName;
            long maxVid = getMaxVid(nodeLabel);
            if (maxVid <= 1) { // maxVid must be > 1 for rand.nextLong(maxVid-1) to be valid
                System.err.println("Max VID is too small. Cannot run basic op test. Is the graph loaded correctly?");
                return;
            }
            Random rand = new Random();

            try (Session session = driver.session()) {
                long duration;

                // 1. Add Vertex
                long start = System.nanoTime();
                for (int i = 0; i < MAX_BASIC_OP; i++) {
                    long vid = maxVid + i + 1; // Use new VIDs to avoid conflicts
                    session.run("CREATE (n:" + nodeLabel + " {id: $id})", Map.of("id", vid));
                }
                duration = System.nanoTime() - start;
                System.out.printf("Add Vertex Latency: %.2f us\n", (double) duration / MAX_BASIC_OP / 1000.0);

                // 2. Add Edge
                start = System.nanoTime();
                for (int i = 0; i < MAX_BASIC_OP; i++) {
                    long src = rand.nextLong(maxVid-1) + 1;
                    long dst = rand.nextLong(maxVid-1) + 1;
                    session.run("MATCH (a:" + nodeLabel + " {id: $src}), (b:" + nodeLabel + " {id: $dst}) CREATE (a)-[:REL]->(b)", Map.of("src", src, "dst", dst));
                }
                duration = System.nanoTime() - start;
                System.out.printf("Add Edge Latency: %.2f us\n", (double) duration / MAX_BASIC_OP / 1000.0);
                
                // 3. Delete Edge
                start = System.nanoTime();
                for (int i = 0; i < MAX_BASIC_OP; i++) {
                    session.run("MATCH ()-[r:REL]->() WITH r, rand() as random ORDER BY random LIMIT 1 DELETE r");
                }
                duration = System.nanoTime() - start;
                System.out.printf("Delete Edge Latency: %.2f us\n", (double) duration / MAX_BASIC_OP / 1000.0);

                // 4. Get Neighbors
                start = System.nanoTime();
                for (int i = 0; i < MAX_BASIC_OP; i++) {
                    long vid = rand.nextLong(maxVid-1) + 1;
                    session.run("MATCH (a:" + nodeLabel + " {id: $vid})-->(b) RETURN b.id", Map.of("vid", vid));
                }
                duration = System.nanoTime() - start;
                System.out.printf("Get Neighbors Latency: %.2f us\n", (double) duration / MAX_BASIC_OP / 1000.0);
            }
        }
        
        public void readWriteTest(double readRatio) {
            System.out.printf("Running throughput test with read ratio %.2f...\n", readRatio);
            String nodeLabel = graphName;
            long maxVid = getMaxVid(nodeLabel);
             if (maxVid <= 1) {
                 System.err.println("Max VID is too small. Cannot run throughput test.");
                 return;
             }
            Random rand = new Random();

            try (Session session = driver.session()) {
                long start = System.nanoTime();
                for (int i = 0; i < MAX_OPS; i++) {
                    if (rand.nextDouble() < readRatio) {
                        // Read Op: Get Neighbors
                        long vid = rand.nextLong(maxVid-1) + 1;
                        session.run("MATCH (a:" + nodeLabel + " {id: $vid})-->(b) RETURN b.id", Map.of("vid", vid));
                    } else {
                        // Write Op: 50/50 chance of Add Vertex or Add Edge
                        if (rand.nextBoolean()) {
                            long vid = maxVid + i + 1;
                            session.run("CREATE (:" + nodeLabel + " {id: $id})", Map.of("id", vid));
                        } else {
                            long src = rand.nextLong(maxVid-1) + 1;
                            long dst = rand.nextLong(maxVid-1) + 1;
                            session.run("MATCH (a:" + nodeLabel + " {id: $src}), (b:" + nodeLabel + " {id: $dst}) CREATE (a)-[:REL]->(b)", Map.of("src", src, "dst", dst));
                        }
                    }
                }
                long duration = System.nanoTime() - start;
                double seconds = duration / 1_000_000_000.0;
                double throughput = MAX_OPS / seconds;
                System.out.printf("Throughput: %.2f op/s\n", throughput);
            }
        }
        
        public void propertyOpTest() {
            System.out.println("Running property operations latency test...");
            String nodeLabel = graphName + "_PropertyV";
            long maxVid = getMaxVid(nodeLabel);
            if (maxVid <= 1) {
                 System.err.println("Max VID is too small. Cannot run property test. Is the property graph loaded?");
                 return;
             }
            Random rand = new Random();
            
            try (Session session = driver.session()) {
                // Add Vertex Property
                long start = System.nanoTime();
                for (int i=0; i < MAX_BASIC_OP; i++) {
                    long vid = rand.nextLong(maxVid-1) + 1;
                    session.run("MATCH (n:" + nodeLabel + " {id: $id}) SET n.newProp = $val", Map.of("id", vid, "val", i));
                }
                long duration = System.nanoTime() - start;
                System.out.printf("Add Vertex Property Latency: %.2f us\n", (double) duration / MAX_BASIC_OP / 1000.0);

                // Delete Vertex Property
                start = System.nanoTime();
                for (int i=0; i < MAX_BASIC_OP; i++) {
                    long vid = rand.nextLong(maxVid-1) + 1;
                    session.run("MATCH (n:" + nodeLabel + " {id: $id}) REMOVE n.newProp", Map.of("id", vid));
                }
                duration = System.nanoTime() - start;
                System.out.printf("Delete Vertex Property Latency: %.2f us\n", (double) duration / MAX_BASIC_OP / 1000.0);
            }
        }

        public void testAlgm(String algm) {
            System.out.println("Testing algorithm(s): " + algm);
            String nodeLabel = graphName;
            String relType = "REL";

            try (Session session = driver.session()) {
                // Step 1: Project graph into GDS
                System.out.println("Projecting graph to GDS...");
                session.run("CALL gds.graph.project($graphName, $nodeLabel, $relType)",
                        Map.of("graphName", graphName, "nodeLabel", nodeLabel, "relType", relType));
                
                // Step 2: Run algorithms
                if ("all".equals(algm) || "pagerank".equals(algm)) {
                    long start = System.currentTimeMillis();
                    session.run("CALL gds.pageRank.stream($graphName) YIELD nodeId, score RETURN count(*)", Map.of("graphName", graphName));
                    System.out.printf("PageRank Done, Time: %.3f s\n", (System.currentTimeMillis() - start) / 1000.0);
                }
                if ("all".equals(algm) || "wcc".equals(algm)) {
                    long start = System.currentTimeMillis();
                    session.run("CALL gds.wcc.stream($graphName) YIELD nodeId, componentId RETURN count(*)", Map.of("graphName", graphName));
                    System.out.printf("WCC Done, Time: %.3f s\n", (System.currentTimeMillis() - start) / 1000.0);
                }
                if ("all".equals(algm) || "cdlp".equals(algm)) {
                    long start = System.currentTimeMillis();
                    session.run("CALL gds.labelPropagation.stream($graphName) YIELD nodeId, communityId RETURN count(*)", Map.of("graphName", graphName));
                    System.out.printf("CDLP Done, Time: %.3f s\n", (System.currentTimeMillis() - start) / 1000.0);
                }
                if ("all".equals(algm) || "sssp".equals(algm)) {
                    long startVid = graphName.equalsIgnoreCase("Wiki-Talk") ? 1 : 1; // Find suitable start nodes
                    long start = System.currentTimeMillis();
                    session.run("MATCH (start:" + nodeLabel + " {id: $startId}) " +
                                "CALL gds.sssp.stream($graphName, { sourceNode: start }) YIELD nodeId, distance RETURN count(*)",
                            Map.of("graphName", graphName, "startId", startVid));
                    System.out.printf("SSSP Done, Time: %.3f s\n", (System.currentTimeMillis() - start) / 1000.0);
                }
                 if ("all".equals(algm) || "bfs".equals(algm)) {
                    long startVid = graphName.equalsIgnoreCase("Wiki-Talk") ? 1 : 1; // Find suitable start nodes
                    long start = System.currentTimeMillis();
                    session.run("MATCH (start:" + nodeLabel + " {id: $startId}) " +
                                "CALL gds.bfs.stream($graphName, { sourceNode: start }) YIELD path RETURN count(*)",
                            Map.of("graphName", graphName, "startId", startVid));
                    System.out.printf("BFS Done, Time: %.3f s\n", (System.currentTimeMillis() - start) / 1000.0);
                }

                // Clean up GDS projection
                session.run("CALL gds.graph.drop($graphName, false)", Map.of("graphName", graphName));
            }
        }
    }

    // --- Main Execution Logic ---
    @Override
    public Integer call() throws Exception {
        try (Neo4jBenchmarker benchmarker = new Neo4jBenchmarker(uri, user, password, graphName)) {
            switch (command) {
                case load_graph:
                    benchmarker.dropGraph();
                    benchmarker.createSchema(false);
                    benchmarker.loadData(dataPath, undirected);
                    break;
                case load_property:
                    benchmarker.dropGraph();
                    benchmarker.createSchema(true);
                    benchmarker.loadPropertyGraph(dataPath);
                    break;
                case test_basic_op:
                    benchmarker.basicOpTest();
                    break;
                case test_read_write_op:
                    benchmarker.readWriteTest(readRatio);
                    break;
                case test_property:
                    benchmarker.propertyOpTest();
                    break;
                case test_algm:
                    benchmarker.testAlgm(algm);
                    break;
                default:
                    System.err.println("Command not implemented: " + command);
                    return 1;
            }
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
        return 0;
    }

    public static void main(String[] args) {
        int exitCode = new CommandLine(new Neo4jBenchmark()).execute(args);
        System.exit(exitCode);
    }
}
