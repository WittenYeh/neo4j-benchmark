import org.neo4j.driver.*;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.nio.file.StandardOpenOption;

/**
 * Handles the core logic for running benchmarks, including data loading,
 * schema management, and various performance tests.
 */
public class BenchmarkRunner implements AutoCloseable {
    private final Driver driver;
    private final String graphName;
    private final ResultLogger resultLogger;

    private final int BATCH_SIZE = 4096; // Batch size for data loading
    private final int OP_BATCH_SIZE = 10240; // Batch size for transactional operations in tests
    private final long MAX_OPS = 20480; // Total operations for throughput test
    private final int MAX_BASIC_OP = 10240; // Operations per latency test

    private final int PROGRESS_BATCH_INTERVAL = 16;
    private final int PROGRESS_OP_INTERVAL = 128;

    public BenchmarkRunner(String uri, String user, String password, String graphName, String resultPath) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
        this.graphName = graphName;
        this.resultLogger = new ResultLogger(resultPath, graphName);
    }
    
    @Override
    public void close() throws Exception {
        resultLogger.writeToFile();
        driver.close();
    }

    // --- Schema and Data Management ---

    public void dropGraph() {
        System.out.println("\n" + "=".repeat(20) + " Dropping Graph " + "=".repeat(20));
        System.out.println("Dropping graph '" + graphName + "'...");
        try (Session session = driver.session()) {
            Result graphExistsResult = session.run("CALL gds.graph.exists($graphName) YIELD exists", Map.of("graphName", graphName));
            if (graphExistsResult.hasNext() && graphExistsResult.single().get("exists").asBoolean()) {
                session.run("CALL gds.graph.drop($graphName, false)", Map.of("graphName", graphName));
                System.out.println("GDS graph projection dropped.");
            } else {
                System.out.println("GDS graph projection did not exist.");
            }
        } catch (Exception e) {
            System.out.println("Could not check/drop GDS projection (GDS plugin might not be installed).");
        }

        try (Session session = driver.session()) {
            String propertyVLabel = graphName + "_PropertyV";
            session.run("DROP INDEX `" + graphName + "_id_index` IF EXISTS");
            session.run("DROP INDEX `" + propertyVLabel + "_id_index` IF EXISTS");
            System.out.println("Indexes dropped (if they existed).");

            System.out.println("Deleting graph data in batches...");
            String deleteQuery = "MATCH (n) WHERE labels(n)[0] STARTS WITH $graphName CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS";
            session.run(deleteQuery, Map.of("graphName", graphName));
            System.out.println("Graph data deleted.");
        }
    }

    public void createSchema(boolean isPropertyGraph) {
        System.out.println("\n" + "=".repeat(20) + " Creating Schema " + "=".repeat(20));
        String nodeLabel = isPropertyGraph ? graphName + "_PropertyV" : graphName;
        try (Session session = driver.session()) {
            session.run("CREATE INDEX `" + nodeLabel + "_id_index` IF NOT EXISTS FOR (n:`" + nodeLabel + "`) ON (n.id)");
            System.out.println("Index on `" + nodeLabel + "`(id) created.");
        }
        System.out.println("Waiting 10 seconds for index to be online...");
        try { TimeUnit.SECONDS.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    public void loadData(String dataPath, boolean isUndirected) throws IOException {
        System.out.println("\n" + "=".repeat(20) + " Loading Graph Data " + "=".repeat(20));
        System.out.println("Loading data from '" + dataPath + "'...");
        long startLoadTime = System.currentTimeMillis();

        Set<Long> nodes = new HashSet<>();
        List<Map<String, Object>> edges = new ArrayList<>();

        long lineCounter = 0;
        System.out.println("Reading data file...");
        final int PROGRESS_INTERVAL = 100000;

        try (BufferedReader reader = new BufferedReader(new FileReader(dataPath))) {
            String line;
            while ((line = reader.readLine()) != null) {
                lineCounter++;
                if (lineCounter % PROGRESS_INTERVAL == 0) System.out.printf("\rProcessed %d lines...", lineCounter);
                if (line.startsWith("#")) continue;
                String[] parts = line.trim().split("\\s+");
                if (parts.length < 2) continue;
                try {
                    long src = Long.parseLong(parts[0]);
                    long dst = Long.parseLong(parts[1]);
                    nodes.add(src);
                    nodes.add(dst);
                    edges.add(Map.of("src", src, "dst", dst));
                    if (isUndirected) edges.add(Map.of("src", dst, "dst", src));
                } catch (NumberFormatException e) { /* Ignore non-numeric lines */ }
            }
        }
        System.out.printf("\rProcessed %d lines... Done.\n", lineCounter);

        List<Map<String, Object>> nodeList = nodes.stream().map(n -> Map.of("id", (Object)n)).collect(Collectors.toList());

        try (Session session = driver.session()) {
            System.out.printf("Loading %d nodes...\n", nodes.size());
            String nodeQuery = "UNWIND $nodes AS node_data MERGE (n:`" + graphName + "` {id: node_data.id})";
            for (int i = 0; i < nodeList.size(); i += BATCH_SIZE) {
                List<Map<String, Object>> batch = nodeList.subList(i, Math.min(i + BATCH_SIZE, nodeList.size()));
                session.run(nodeQuery, Map.of("nodes", batch));
                int processed = Math.min(i + BATCH_SIZE, nodeList.size());
                if (processed % (BATCH_SIZE * PROGRESS_BATCH_INTERVAL) == 0 || processed == nodeList.size()) {
                    System.out.printf("\rInserted nodes: %d / %d (%.2f%%)", processed, nodeList.size(), (double) processed / nodeList.size() * 100);
                }
            }
            System.out.println();

            System.out.printf("Loading %d relationships...\n", edges.size());
            String edgeQuery = "UNWIND $edges AS edge MATCH (src:`" + graphName + "` {id: edge.src}), (dst:`" + graphName + "` {id: edge.dst}) MERGE (src)-[:REL]->(dst)";
            for (int i = 0; i < edges.size(); i += BATCH_SIZE) {
                List<Map<String, Object>> batch = edges.subList(i, Math.min(i + BATCH_SIZE, edges.size()));
                session.run(edgeQuery, Map.of("edges", batch));
                int processed = Math.min(i + BATCH_SIZE, edges.size());
                if (processed % (BATCH_SIZE * PROGRESS_BATCH_INTERVAL) == 0 || processed == edges.size()) {
                     System.out.printf("\rInserted relationships: %d / %d (%.2f%%)", processed, edges.size(), (double) processed / edges.size() * 100);
                }
            }
            System.out.println();
        }

        long endLoadTime = System.currentTimeMillis();
        double durationSeconds = (endLoadTime - startLoadTime) / 1000.0;
        
        resultLogger.logSectionHeader("Data Loading Summary");
        resultLogger.log("Total time: %.3f seconds", durationSeconds);
        if (durationSeconds > 0) {
            resultLogger.log("Node loading throughput: %.2f nodes/sec", nodes.size() / durationSeconds);
            resultLogger.log("Edge loading throughput: %.2f edges/sec", edges.size() / durationSeconds);
        }
    }
    
    public void loadPropertyGraph(String dataPath) throws IOException {
        System.out.println("\n" + "=".repeat(20) + " Loading Property Graph Data " + "=".repeat(20));
        long startLoadTime = System.currentTimeMillis();
        String vertexLabel = graphName + "_PropertyV";
        String edgeType = graphName + "_PropertyE";
        Path vertexFile = Paths.get(dataPath + ".vertex");
        Path edgeFile = Paths.get(dataPath + ".edge");

        if (!Files.exists(vertexFile) || !Files.exists(edgeFile)) {
            throw new IOException("Data files not found! Expected: " + vertexFile + " and " + edgeFile);
        }

        List<Map<String, Object>> vertices;
        List<Map<String, Object>> edges;

        try(Session session = driver.session()) {
            System.out.printf("Loading vertices from %s\n", vertexFile);
            vertices = new ArrayList<>();
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
             System.out.printf("Inserting %d vertices...\n", vertices.size());
             for (int i = 0; i < vertices.size(); i += BATCH_SIZE) {
                 List<Map<String, Object>> batch = vertices.subList(i, Math.min(i + BATCH_SIZE, vertices.size()));
                 session.run("UNWIND $batch as props CREATE (n:`" + vertexLabel + "`) SET n = props", Map.of("batch", batch));
                 int processed = Math.min(i + BATCH_SIZE, vertices.size());
                 if (processed % (BATCH_SIZE * PROGRESS_BATCH_INTERVAL) == 0 || processed == vertices.size()) {
                    System.out.printf("\rInserting vertices: %d / %d (%.2f%%)", processed, vertices.size(), (double) processed / vertices.size() * 100);
                 }
             }
             System.out.println();

            System.out.printf("Loading edges from %s\n", edgeFile);
            edges = new ArrayList<>();
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
            System.out.printf("Inserting %d edges...\n", edges.size());
            for (int i = 0; i < edges.size(); i += BATCH_SIZE) {
                List<Map<String, Object>> batch = edges.subList(i, Math.min(i + BATCH_SIZE, edges.size()));
                session.run("UNWIND $batch as edge " +
                            "MATCH (src:`" + vertexLabel + "` {id: edge.src}) " +
                            "MATCH (dst:`" + vertexLabel + "` {id: edge.dst}) " +
                            "CREATE (src)-[r:`" + edgeType + "`]->(dst) SET r = edge.props", Map.of("batch", batch));
                int processed = Math.min(i + BATCH_SIZE, edges.size());
                if (processed % (BATCH_SIZE * PROGRESS_BATCH_INTERVAL) == 0 || processed == edges.size()) {
                    System.out.printf("\rInserting edges: %d / %d (%.2f%%)", processed, edges.size(), (double) processed / edges.size() * 100);
                }
            }
            System.out.println();
        }

        long endLoadTime = System.currentTimeMillis();
        double durationSeconds = (endLoadTime - startLoadTime) / 1000.0;
        resultLogger.logSectionHeader("Property Graph Loading Summary");
        resultLogger.log("Total time: %.3f seconds", durationSeconds);
        if (durationSeconds > 0) {
            resultLogger.log("Vertex loading throughput: %.2f vertices/sec", vertices.size() / durationSeconds);
            resultLogger.log("Edge loading throughput: %.2f edges/sec", edges.size() / durationSeconds);
        }
    }

    private long getMaxVid(String nodeLabel) {
        try (Session session = driver.session()) {
            Result result = session.run("MATCH (n:`" + nodeLabel + "`) RETURN max(n.id) AS maxId");
            if (result.hasNext()) {
                Value maxIdValue = result.single().get("maxId");
                if (!maxIdValue.isNull()) return maxIdValue.asLong();
            }
            return 0L;
        }
    }

    public void basicOpTest() {
        System.out.println("\n" + "=".repeat(20) + " Basic Operations Latency Test " + "=".repeat(20));
        resultLogger.logSectionHeader("Basic Operations Latency Test");
        String nodeLabel = graphName;
        long maxVid = getMaxVid(nodeLabel);
        if (maxVid <= 1) {
            String errorMsg = "ERROR: Max VID is too small. Cannot run basic op test. Is the graph loaded correctly?";
            System.err.println(errorMsg);
            resultLogger.log(errorMsg);
            return;
        }
        Random rand = new Random();
        List<Long> latencies = new ArrayList<>(MAX_BASIC_OP);
        long start, end;

        try (Session session = driver.session()) {
            // 1. Add Vertex (Batched)
            List<Map<String, Object>> batch = new ArrayList<>(OP_BATCH_SIZE);
            for (int i = 0; i < MAX_BASIC_OP; i++) {
                long vid = maxVid + i + 1;
                batch.add(Map.of("id", vid));
                if (batch.size() == OP_BATCH_SIZE || i == MAX_BASIC_OP - 1) {
                    start = System.nanoTime();
                    session.writeTransaction(tx -> {
                        tx.run("UNWIND $batch as props CREATE (n:`" + nodeLabel + "` {id: props.id})", Map.of("batch", batch));
                        return null;
                    });
                    end = System.nanoTime();
                    long avgLatency = (end - start) / batch.size();
                    for(int j=0; j<batch.size(); j++) latencies.add(avgLatency);
                    batch.clear();
                }
                 if ((i+1) % PROGRESS_OP_INTERVAL == 0 || (i+1) == MAX_BASIC_OP) System.out.printf("\rAdd Vertex: %d / %d", i+1, MAX_BASIC_OP);
            }
            System.out.println();
            resultLogger.log("\n--- Operation: Add Vertex ---");
            resultLogger.log("Total Operations: %d", MAX_BASIC_OP);
            resultLogger.log(LatencyStats.fromNanos(latencies).toString());
            latencies.clear();

            // 2. Add Edge (Batched)
            for (int i = 0; i < MAX_BASIC_OP; i++) {
                long src = rand.nextLong(maxVid) + 1;
                long dst = rand.nextLong(maxVid) + 1;
                batch.add(Map.of("src", src, "dst", dst));
                if (batch.size() == OP_BATCH_SIZE || i == MAX_BASIC_OP - 1) {
                    start = System.nanoTime();
                    session.writeTransaction(tx -> {
                       tx.run("UNWIND $batch as edge MATCH (a:`" + nodeLabel + "` {id: edge.src}), (b:`" + nodeLabel + "` {id: edge.dst}) CREATE (a)-[:REL]->(b)", Map.of("batch", batch));
                       return null;
                    });
                    end = System.nanoTime();
                    long avgLatency = (end - start) / batch.size();
                    for(int j=0; j<batch.size(); j++) latencies.add(avgLatency);
                    batch.clear();
                }
                if ((i+1) % PROGRESS_OP_INTERVAL == 0 || (i+1) == MAX_BASIC_OP) System.out.printf("\rAdd Edge: %d / %d", i+1, MAX_BASIC_OP);
            }
            System.out.println();
            resultLogger.log("\n--- Operation: Add Edge ---");
            resultLogger.log("Total Operations: %d", MAX_BASIC_OP);
            resultLogger.log(LatencyStats.fromNanos(latencies).toString());
            latencies.clear();
            
            // 3. Delete Edge (Batched)
            long successfulDeletions = 0;
            List<Map<String, Object>> edgesToDelete = new ArrayList<>(OP_BATCH_SIZE);
            for (int i = 0; i < MAX_BASIC_OP; i++) {
                // Find edges to delete first
                while(edgesToDelete.size() < OP_BATCH_SIZE && (i + edgesToDelete.size()) < MAX_BASIC_OP) {
                     long src = rand.nextLong(maxVid) + 1;
                     Result findNeighborResult = session.run("MATCH (a:`" + nodeLabel + "` {id: $src})-[:REL]->(b) RETURN b.id AS dstId LIMIT 1", Map.of("src", src));
                     if (findNeighborResult.hasNext()) {
                         long dstId = findNeighborResult.single().get("dstId").asLong();
                         edgesToDelete.add(Map.of("src", src, "dst", dstId));
                     }
                }

                if (!edgesToDelete.isEmpty()) {
                    start = System.nanoTime();
                    session.writeTransaction(tx -> {
                        tx.run("UNWIND $edges as edge MATCH (a:`" + nodeLabel + "` {id: edge.src})-[r:REL]->(b:`" + nodeLabel + "` {id: edge.dst}) WITH r LIMIT 1 DELETE r", Map.of("edges", edgesToDelete));
                        return null;
                    });
                    end = System.nanoTime();
                    long avgLatency = (end - start) / edgesToDelete.size();
                    for(int j=0; j<edgesToDelete.size(); j++) latencies.add(avgLatency);
                    successfulDeletions += edgesToDelete.size();
                    i += edgesToDelete.size() - 1; // Advance outer loop counter
                    edgesToDelete.clear();
                }
                if ((i+1) % PROGRESS_OP_INTERVAL == 0 || (i+1) >= MAX_BASIC_OP) System.out.printf("\rDelete Edge: %d / %d", Math.min(i+1, MAX_BASIC_OP), MAX_BASIC_OP);
            }
            System.out.println();
            resultLogger.log("\n--- Operation: Delete Edge ---");
            resultLogger.log("Attempted Operations: %d", MAX_BASIC_OP);
            resultLogger.log("Successful Deletions: %d", successfulDeletions);
            resultLogger.log(LatencyStats.fromNanos(latencies).toString());
            latencies.clear();

            // 4. Get Neighbors (Read-only, no batching needed for transactions)
            for (int i = 0; i < MAX_BASIC_OP; i++) {
                long vid = rand.nextLong(maxVid) + 1;
                start = System.nanoTime();
                // Use readTransaction for read-only queries
                session.readTransaction(tx -> tx.run("MATCH (a:`" + nodeLabel + "` {id: $vid})-->(b) RETURN b.id", Map.of("vid", vid)).consume());
                end = System.nanoTime();
                latencies.add(end - start);
                if ((i+1) % PROGRESS_OP_INTERVAL == 0 || (i+1) == MAX_BASIC_OP) System.out.printf("\rGet Neighbors: %d / %d", i+1, MAX_BASIC_OP);
            }
            System.out.println();
            resultLogger.log("\n--- Operation: Get Neighbors ---");
            resultLogger.log("Total Operations: %d", MAX_BASIC_OP);
            resultLogger.log(LatencyStats.fromNanos(latencies).toString());
        }
    }
    
    public void readWriteTest(double readRatio) {
        System.out.println("\n" + "=".repeat(20) + " Read/Write Throughput Test " + "=".repeat(20));
        resultLogger.logSectionHeader("Read/Write Throughput Test");
        resultLogger.log("Configuration: Total Ops=%d, Read Ratio=%.2f, Write Batch Size=%d", MAX_OPS, readRatio, OP_BATCH_SIZE);
        String nodeLabel = graphName;
        long duration;

        long maxVid = getMaxVid(nodeLabel);
         if (maxVid <= 1) {
             String errorMsg = "ERROR: Max VID is too small. Cannot run throughput test.";
             System.err.println(errorMsg);
             resultLogger.log(errorMsg);
             return;
         }
        System.out.printf("Max VID found: %d\n", maxVid);
        Random rand = new Random();

        System.out.println("Generating and shuffling operation list...");
        List<Integer> ops = new ArrayList<>((int) MAX_OPS);
        long readOpsCount = (long) (MAX_OPS * readRatio);
        for (long i = 0; i < readOpsCount; i++) ops.add(0); // 0 = read
        for (long i = 0; i < MAX_OPS - readOpsCount; i++) ops.add(1); // 1 = write
        Collections.shuffle(ops);
        System.out.println("Operation list generated. Starting test...");

        try (Session session = driver.session()) {
            long opsCounter = 0;
            long testStartNanos = System.nanoTime();
            List<Map<String, Object>> writeBatch = new ArrayList<>(OP_BATCH_SIZE);

            for (int op : ops) {
                if (op == 0) { // Read
                    long vid = rand.nextLong(maxVid) + 1;
                    session.readTransaction(tx -> tx.run("MATCH (a:`" + nodeLabel + "` {id: $vid})-->(b) RETURN b.id", Map.of("vid", vid)).consume());
                } else { // Write - add to batch
                    long src = rand.nextLong(maxVid) + 1;
                    long dst = rand.nextLong(maxVid) + 1;
                    writeBatch.add(Map.of("src", src, "dst", dst));
                    if (writeBatch.size() >= OP_BATCH_SIZE) {
                        final List<Map<String, Object>> batchToCommit = new ArrayList<>(writeBatch);
                        session.writeTransaction(tx -> {
                            tx.run("UNWIND $batch as edge MATCH (a:`" + nodeLabel + "` {id: edge.src}), (b:`" + nodeLabel + "` {id: edge.dst}) CREATE (a)-[:REL]->(b)", Map.of("batch", batchToCommit));
                            return null;
                        });
                        writeBatch.clear();
                    }
                }
                opsCounter++;
                if (opsCounter % (PROGRESS_OP_INTERVAL * 10) == 0 || opsCounter == MAX_OPS) {
                    System.out.printf("\rExecuting Ops: %d / %d (%.2f%%)", opsCounter, MAX_OPS, (double) opsCounter / MAX_OPS * 100);
                }
            }
            // Commit any remaining writes in the batch
            if (!writeBatch.isEmpty()) {
                 final List<Map<String, Object>> batchToCommit = new ArrayList<>(writeBatch);
                 session.writeTransaction(tx -> {
                    tx.run("UNWIND $batch as edge MATCH (a:`" + nodeLabel + "` {id: edge.src}), (b:`" + nodeLabel + "` {id: edge.dst}) CREATE (a)-[:REL]->(b)", Map.of("batch", batchToCommit));
                    return null;
                });
            }

            duration = System.nanoTime() - testStartNanos;
            System.out.println();
        }

        double seconds = duration / 1_000_000_000.0;
        resultLogger.log("\n--- Throughput Test Summary ---");
        resultLogger.log("Total operations executed: %d", MAX_OPS);
        resultLogger.log("Total time: %.3f seconds", seconds);
        if (seconds > 0) {
            resultLogger.log("Throughput: %.2f op/s", MAX_OPS / seconds);
        } else {
            resultLogger.log("Throughput: N/A (duration too short to measure)");
        }
    }
    
    public void propertyOpTest() {
        System.out.println("\n" + "=".repeat(20) + " Property Operations Latency Test " + "=".repeat(20));
        resultLogger.logSectionHeader("Property Operations Latency Test");
        String nodeLabel = graphName + "_PropertyV";
        long maxVid = getMaxVid(nodeLabel);
        if (maxVid <= 1) {
            String errorMsg = "ERROR: Max VID is too small. Is the property graph loaded?";
            System.err.println(errorMsg);
            resultLogger.log(errorMsg);
            return;
        }
        Random rand = new Random();
        List<Long> latencies = new ArrayList<>(MAX_BASIC_OP);
        long start, end;
        
        try (Session session = driver.session()) {
            List<Map<String, Object>> batch = new ArrayList<>(OP_BATCH_SIZE);
            
            // Add Vertex Property (Batched)
            for (int i=0; i < MAX_BASIC_OP; i++) {
                long vid = rand.nextLong(maxVid) + 1;
                batch.add(Map.of("id", vid, "val", i));
                if(batch.size() == OP_BATCH_SIZE || i == MAX_BASIC_OP - 1) {
                    start = System.nanoTime();
                    session.writeTransaction(tx -> {
                        tx.run("UNWIND $batch as item MATCH (n:`" + nodeLabel + "` {id: item.id}) SET n.newProp = item.val", Map.of("batch", batch));
                        return null;
                    });
                    end = System.nanoTime();
                    long avgLatency = (end - start) / batch.size();
                    for(int j=0; j<batch.size(); j++) latencies.add(avgLatency);
                    batch.clear();
                }
                if ((i+1) % PROGRESS_OP_INTERVAL == 0 || (i+1) == MAX_BASIC_OP) System.out.printf("\rAdd Property: %d / %d", i+1, MAX_BASIC_OP);
            }
            System.out.println();
            resultLogger.log("\n--- Operation: Add Vertex Property ---");
            resultLogger.log("Total Operations: %d", MAX_BASIC_OP);
            resultLogger.log(LatencyStats.fromNanos(latencies).toString());
            latencies.clear();

            // Delete Vertex Property (Batched)
            for (int i=0; i < MAX_BASIC_OP; i++) {
                long vid = rand.nextLong(maxVid) + 1;
                batch.add(Map.of("id", vid));
                 if(batch.size() == OP_BATCH_SIZE || i == MAX_BASIC_OP - 1) {
                    start = System.nanoTime();
                    session.writeTransaction(tx -> {
                        tx.run("UNWIND $batch as item MATCH (n:`" + nodeLabel + "` {id: item.id}) REMOVE n.newProp", Map.of("batch", batch));
                        return null;
                    });
                    end = System.nanoTime();
                    long avgLatency = (end - start) / batch.size();
                    for(int j=0; j<batch.size(); j++) latencies.add(avgLatency);
                    batch.clear();
                }
                if ((i+1) % PROGRESS_OP_INTERVAL == 0 || (i+1) == MAX_BASIC_OP) System.out.printf("\rDelete Property: %d / %d", i+1, MAX_BASIC_OP);
            }
            System.out.println();
            resultLogger.log("\n--- Operation: Delete Vertex Property ---");
            resultLogger.log("Total Operations: %d", MAX_BASIC_OP);
            resultLogger.log(LatencyStats.fromNanos(latencies).toString());
        }
    }

    /**
     * Runs all GDS algorithm tests using a consistent, throughput-based methodology.
     * Each algorithm is run multiple times to calculate an average throughput in edges/sec.
     * The process of counting the edges for the throughput calculation is done
     * outside of the timed execution loop.
     *
     * @param algm The algorithm(s) to test (currently supports "all" or specific names).
     */
    public void testAlgm(String algm) {
        System.out.println("\n" + "=".repeat(20) + " GDS Algorithm Throughput Test " + "=".repeat(20));
        resultLogger.logSectionHeader("GDS Algorithm Throughput Test");
        resultLogger.log("Testing algorithm(s): " + algm);
        
        final int RUNS = 10; // Number of times to repeat each algorithm test for averaging.
        final String nodeLabel = this.graphName; 
        final String relType = "REL";

        try (Session session = driver.session()) {
            // --- Step 1: Project graph into GDS ---
            System.out.println("Projecting graph to GDS in-memory store...");
            session.run("CALL gds.graph.project($graphName, $nodeLabel, $relType)",
                    Map.of("graphName", graphName, "nodeLabel", nodeLabel, "relType", relType));
            System.out.println("Graph projected successfully.");

            // --- Step 2: Pre-calculate edge counts (workloads) ---

            // For full-graph algorithms, the workload is the total number of edges.
            System.out.println("Fetching total edge count for full-graph algorithms...");
            Result totalEdgeCountResult = session.run("MATCH ()-[:`" + relType + "`]->() RETURN count(*) AS edgeCount");
            long totalEdgeCount = totalEdgeCountResult.hasNext() ? totalEdgeCountResult.single().get("edgeCount").asLong() : 0;
            System.out.printf("Graph has %d total edges.\n", totalEdgeCount);

            // For traversal algorithms, prepare parameters and calculate the workload.
            long traversedEdgeCount = 0;
            Map<String, Object> traversalParams = new HashMap<>();
            traversalParams.put("graphName", graphName);

            System.out.println("Finding a start node for traversal algorithms...");
            Result startNodeResult = session.run("MATCH (n:`" + nodeLabel + "`) RETURN id(n) AS id LIMIT 1");
            if (startNodeResult.hasNext()) {
                long startNodeId = startNodeResult.single().get("id").asLong();
                traversalParams.put("startNodeId", startNodeId);
                System.out.printf("Found start node with internal ID: %d.\n", startNodeId);

                // --- NEW LOGIC for targetNodes ---
                // For gds.shortestPath.* we must provide target nodes. To simulate SSSP,
                // we provide ALL other nodes as targets.
                System.out.println("Collecting all other nodes as targets for Dijkstra...");
                Result targetNodesResult = session.run(
                    "MATCH (n:`" + nodeLabel + "`) WHERE id(n) <> $startNodeId RETURN collect(id(n)) as targetIds",
                    Map.of("startNodeId", startNodeId)
                );
                List<Object> targetNodeIds = targetNodesResult.hasNext() ? targetNodesResult.single().get("targetIds").asList() : Collections.emptyList();
                traversalParams.put("targetNodeIds", targetNodeIds);
                System.out.printf("Found %d target nodes.\n", targetNodeIds.size());

                // The workload (traversed edges) is still based on the reachable subgraph from the source.
                System.out.println("Pre-calculating traversed edge count from start node...");
                String reachableNodesQuery =
                    "CALL gds.bfs.stream($graphName, { sourceNode: $startNodeId }) " +
                    "YIELD path " +
                    "UNWIND nodes(path) AS node " +
                    "RETURN collect(DISTINCT node.id) AS nodeIds";
                Result reachableNodesResult = session.run(reachableNodesQuery, traversalParams);
                
                List<Object> reachableNodePropertyIds = reachableNodesResult.hasNext() ? reachableNodesResult.single().get("nodeIds").asList() : Collections.emptyList();

                if (!reachableNodePropertyIds.isEmpty()) {
                    Result traversedEdgeCountResult = session.run(
                        "MATCH (n:`" + nodeLabel + "`)-[r:`" + relType + "`]->(m:`" + nodeLabel + "`) " +
                        "WHERE n.id IN $nodeIds AND m.id IN $nodeIds " +
                        "RETURN count(r) as edgeCount",
                        Map.of("nodeIds", reachableNodePropertyIds)
                    );
                    traversedEdgeCount = traversedEdgeCountResult.hasNext() ? traversedEdgeCountResult.single().get("edgeCount").asLong() : 0;
                }
                System.out.printf("Reachable subgraph from start node contains %d edges.\n", traversedEdgeCount);
            } else {
                System.err.println("Warning: Could not find a start node. Traversal tests will be skipped.");
            }

            // --- Step 3: Define and Run Algorithm Tests ---

            if ("all".equals(algm) || "pagerank".equals(algm)) {
                String query = "CALL gds.pageRank.stream($graphName) YIELD nodeId, score RETURN count(*)";
                executeAndMeasureThroughput(session, "PageRank", query, Map.of("graphName", graphName), totalEdgeCount, RUNS);
            }
            if ("all".equals(algm) || "wcc".equals(algm)) {
                String query = "CALL gds.wcc.stream($graphName) YIELD nodeId, componentId RETURN count(*)";
                executeAndMeasureThroughput(session, "WCC", query, Map.of("graphName", graphName), totalEdgeCount, RUNS);
            }
            if ("all".equals(algm) || "cdlp".equals(algm)) {
                String query = "CALL gds.labelPropagation.stream($graphName) YIELD nodeId, communityId RETURN count(*)";
                executeAndMeasureThroughput(session, "CDLP", query, Map.of("graphName", graphName), totalEdgeCount, RUNS);
            }

            // Run traversal tests only if a start node was found.
            if (traversalParams.containsKey("startNodeId")) {
                if ("all".equals(algm) || "dijkstra_sssp".equals(algm)) {
                    // --- FIX: Use the correct procedure 'gds.shortestPath.dijkstra.stream' ---
                    // Provide both 'sourceNode' and the collected 'targetNodes' list to simulate an SSSP run.
                    String query = "CALL gds.shortestPath.dijkstra.stream($graphName, { sourceNode: $startNodeId, targetNodes: $targetNodeIds }) YIELD targetNode, totalCost RETURN count(*)";
                    executeAndMeasureThroughput(session, "Dijkstra (SSSP simulation)", query, traversalParams, traversedEdgeCount, RUNS);
                }
                if ("all".equals(algm) || "bfs".equals(algm)) {
                    String query = "CALL gds.bfs.stream($graphName, { sourceNode: $startNodeId }) YIELD path RETURN count(*)";
                    executeAndMeasureThroughput(session, "BFS", query, traversalParams, traversedEdgeCount, RUNS);
                }
            } else {
                resultLogger.log("\n--- Traversal Algorithm Summary ---");
                resultLogger.log("SKIPPED: No start node found in the graph.");
            }

            // --- Final Step: Clean up GDS projection ---
            System.out.println("Cleaning up GDS graph projection...");
            session.run("CALL gds.graph.drop($graphName, false)", Map.of("graphName", graphName));
        }
    }

    private void executeAndMeasureThroughput(Session session, String testName, String cypherQuery, Map<String, Object> params, long edgeCountForCalc, int runs) {
        System.out.printf("\nRunning %s throughput test (%d iterations)...\n", testName, runs);
        List<Long> timings = new ArrayList<>();

        for (int i = 0; i < runs; i++) {
            System.out.printf("\r%s run %d/%d...", testName, i + 1, runs);
            long start = System.currentTimeMillis();
            session.run(cypherQuery, params).consume();
            long end = System.currentTimeMillis();
            timings.add(end - start);
        }
        System.out.println();
        logThroughputResults(testName, timings, edgeCountForCalc);
    }
    
    private void logThroughputResults(String testName, List<Long> timings, long totalEdgeCount) {
        if (timings.isEmpty()) return;

        double totalTimeSeconds = timings.stream().mapToLong(Long::longValue).sum() / 1000.0;
        double averageTimeSeconds = totalTimeSeconds / timings.size();
        
        resultLogger.log("\n--- %s Throughput Summary ---", testName);
        resultLogger.log("Total Iterations: %d", timings.size());
        resultLogger.log("Average Execution Time: %.3f s", averageTimeSeconds);

        if (averageTimeSeconds > 0 && totalEdgeCount > 0) {
            double throughput = totalEdgeCount / averageTimeSeconds;
            resultLogger.log("Average Throughput: %.2f edges/sec", throughput);
        } else {
            resultLogger.log("Average Throughput: N/A (zero time or no edges)");
        }
    }

    private static class LatencyStats {
        long count;
        double min_us, max_us, avg_us, p50_us, p95_us, p99_us;

        public static LatencyStats fromNanos(List<Long> timings) {
            LatencyStats stats = new LatencyStats();
            if (timings == null || timings.isEmpty()) {
                stats.count = 0;
                return stats;
            }
            stats.count = timings.size();
            Collections.sort(timings);
            
            long totalNanos = timings.stream().mapToLong(Long::longValue).sum();

            stats.avg_us = (double) totalNanos / stats.count / 1000.0;
            stats.min_us = (double) timings.get(0) / 1000.0;
            stats.max_us = (double) timings.get((int) (stats.count - 1)) / 1000.0;
            stats.p50_us = (double) timings.get((int) (stats.count * 0.50)) / 1000.0;
            stats.p95_us = (double) timings.get((int) (stats.count * 0.95)) / 1000.0;
            stats.p99_us = (double) timings.get((int) (stats.count * 0.99)) / 1000.0;
            return stats;
        }

        @Override
        public String toString() {
            if (count == 0) return "Latency: N/A (no successful operations)";
            return String.format(Locale.US,
                "Avg Latency: %.2f us%n" +
                "Min Latency: %.2f us%n" +
                "Max Latency: %.2f us%n" +
                "p50 Latency: %.2f us%n" +
                "p95 Latency: %.2f us%n" +
                "p99 Latency: %.2f us",
                avg_us, min_us, max_us, p50_us, p95_us, p99_us
            );
        }
    }

    private static class ResultLogger {
        private final String resultPath;
        private final StringBuilder resultBuilder = new StringBuilder();

        /**
         * Constructor for the ResultLogger.
         * It checks if the result file exists. If not, it adds a header to the buffer.
         */
        public ResultLogger(String resultPath, String graphName) {
            this.resultPath = resultPath;
            // --- FIX: Only add the header if the file does not exist ---
            Path path = Paths.get(resultPath);
            if (!Files.exists(path)) {
                resultBuilder.append("=================================================").append(System.lineSeparator());
                resultBuilder.append("        Neo4j Benchmark Results").append(System.lineSeparator());
                resultBuilder.append("=================================================").append(System.lineSeparator());
                resultBuilder.append("Graph Name: ").append(graphName).append(System.lineSeparator());
                resultBuilder.append("-------------------------------------------------").append(System.lineSeparator());
            }
        }

        /** Appends a formatted string to the internal result buffer. */
        public void log(String format, Object... args) {
            String line = String.format(Locale.US, format, args);
            if (resultPath != null) {
                resultBuilder.append(line).append(System.lineSeparator());
            }
        }

        /** Appends a formatted section header to the result buffer. */
        public void logSectionHeader(String header) {
            log("\n" + "=".repeat(20) + " " + header + " " + "=".repeat(20));
        }

        /**
         * Writes the buffered results to the specified file in APPEND mode.
         */
        public void writeToFile() throws IOException {
            if (resultPath != null && resultBuilder.length() > 0) {
                try {
                    // --- FIX: Use APPEND and CREATE options to avoid overwriting ---
                    Files.write(
                        Paths.get(resultPath), 
                        resultBuilder.toString().getBytes(), 
                        StandardOpenOption.APPEND, 
                        StandardOpenOption.CREATE
                    );
                    System.out.println("\nResults for this phase appended to: " + resultPath);
                } catch (IOException e) {
                    System.err.println("FATAL: Failed to write results to file: " + resultPath);
                    throw e;
                }
            }
        }
    }
}