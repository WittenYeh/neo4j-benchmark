import org.neo4j.driver.*;
import org.neo4j.driver.exceptions.Neo4jException;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class BenchmarkRunner implements AutoCloseable {
    private final Driver driver;
    private final String graphName;
    private final int BATCH_SIZE = 2048;
    private final long MAX_OPS = 20000; // Total operations for throughput test
    private final int MAX_BASIC_OP = 1000; // Operations per latency test

    // Define the interval for progress updates (e.g., update every 50 batches)
    private final int PROGRESS_BATCH_INTERVAL = 10;
    private final int PROGRESS_OP_INTERVAL = 100;

    public BenchmarkRunner(String uri, String user, String password, String graphName) {
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
        System.out.println("Reading data file...");
        final int PROGRESS_INTERVAL = 100000; // Update progress every 100,000 lines

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
        if (maxVid <= 1) {
            System.err.println("Max VID is too small. Cannot run basic op test. Is the graph loaded correctly?");
            return;
        }
        Random rand = new Random();

        try (Session session = driver.session()) {
            final int PROGRESS_INTERVAL = 100;

            long duration, start, end;

            // 1. Add Vertex
            duration = 0;
            for (int i = 0; i < MAX_BASIC_OP; i++) {
                long vid =  rand.nextLong() % (maxVid - 1) + 1; // Use new VIDs to avoid conflicts
                start = System.nanoTime();
                session.run("CREATE (n:" + nodeLabel + " {id: $id})", Map.of("id", vid));
                end = System.nanoTime();
                duration += end - start;
                if (i % PROGRESS_OP_INTERVAL == 0 || i == MAX_BASIC_OP-1) {
                    double percentage = i==MAX_BASIC_OP-1 ? 100.0 : (double) (i+1) / MAX_BASIC_OP * 100;
                    System.out.printf("\rInserting edges: %d / %d (%.2f%%)", i+1, MAX_BASIC_OP, percentage);
                }
            }
            System.out.printf("Add Vertex Latency: %.2f us\n", (double) duration / MAX_BASIC_OP / 1000.0);

            // 2. Add Edge
            duration = 0;
            for (int i = 0; i < MAX_BASIC_OP; i++) {
                long src = rand.nextLong() % (maxVid - 1) + 1;
                long dst = rand.nextLong() % (maxVid - 1) + 1;
                start = System.nanoTime();
                session.run("MATCH (a:" + nodeLabel + " {id: $src}), (b:" + nodeLabel + " {id: $dst}) CREATE (a)-[:REL]->(b)", Map.of("src", src, "dst", dst));
                end = System.nanoTime();
                duration += end - start;

                if (i % PROGRESS_OP_INTERVAL == 0 || i == MAX_BASIC_OP-1) {
                    double percentage = i==MAX_BASIC_OP-1 ? 100.0 : (double) (i+1) / MAX_BASIC_OP * 100;
                    System.out.printf("\rInserting edges: %d / %d (%.2f%%)", i+1, MAX_BASIC_OP, percentage);
                }
            }
            System.out.printf("Add Edge Latency: %.2f us\n", (double) duration / MAX_BASIC_OP / 1000.0);
            
            // 3. Delete Edge
            // This method times only the DELETE operation itself, excluding the time to find a neighbor.
            duration = 0;
            long successfulDeletions = 0;
            
            for (int i = 0; i < MAX_BASIC_OP; i++) {
                // Step 1: In Java, pick a random source node.
                long src = rand.nextLong() % (maxVid - 1) + 1;

                // Step 2: Find a single neighbor (dst) of this source node.
                // The time taken for this query is NOT included in the final latency measurement.
                Result findNeighborResult = session.run(
                    "MATCH (a:" + nodeLabel + " {id: $src})-[:REL]->(b) " +
                    "RETURN b.id AS dstId LIMIT 1",
                    Map.of("src", src)
                );

                // Step 3: If a neighbor was found, proceed to delete the edge.
                if (findNeighborResult.hasNext()) {
                    long dstId = findNeighborResult.single().get("dstId").asLong();
                    
                    // Step 4: Time ONLY the execution of the DELETE query.
                    start = System.nanoTime();
                    session.run(
                        "MATCH (a:" + nodeLabel + " {id: $src})-[r:REL]->(b:" + nodeLabel + " {id: $dst}) " +
                        "WITH r LIMIT 1 DELETE r",
                        Map.of("src", src, "dst", dstId)
                    );
                    end = System.nanoTime();

                    // Accumulate the duration and count of successful deletions.
                    duration += (end - start);
                    successfulDeletions++;
                }

                if (i % PROGRESS_OP_INTERVAL == 0 || i == MAX_BASIC_OP-1) {
                    double percentage = i==MAX_BASIC_OP-1 ? 100.0 : (double) (i+1) / MAX_BASIC_OP * 100;
                    System.out.printf("\rInserting edges: %d / %d (%.2f%%)", i+1, MAX_BASIC_OP, percentage);
                }
            }

            // Calculate latency based only on the accumulated time of successful delete operations.
            if (successfulDeletions > 0) {
                System.out.printf("Delete Edge Latency: %.2f us (based on %d successful deletions)\n",
                        (double) duration / successfulDeletions / 1000.0, successfulDeletions);
            } else {
                System.out.println("Delete Edge Latency: N/A (no edges were found to delete)");
            }

            // 4. Get Neighbors
            duration = 0;
            for (int i = 0; i < MAX_BASIC_OP; i++) {
                long vid = rand.nextLong() % (maxVid - 1) + 1;
                start = System.nanoTime();
                session.run("MATCH (a:" + nodeLabel + " {id: $vid})-->(b) RETURN b.id", Map.of("vid", vid));
                end = System.nanoTime();
                duration += (end - start);

                if (i % PROGRESS_OP_INTERVAL == 0 || i == MAX_BASIC_OP-1) {
                    double percentage = i==MAX_BASIC_OP-1 ? 100.0 : (double) (i+1) / MAX_BASIC_OP * 100;
                    System.out.printf("\rInserting edges: %d / %d (%.2f%%)", i+1, MAX_BASIC_OP, percentage);
                }
            }
            System.out.printf("Get Neighbors Latency: %.2f us\n", (double) duration / MAX_BASIC_OP / 1000.0);
        }
    }
    
    public void readWriteTest(double readRatio) {
        System.out.printf("Running throughput test with read ratio %.2f...\n", readRatio);
        String nodeLabel = graphName;
        long start, end, duration = 0;

        // --- 模仿 Nebula Benchmark 逻辑: 获取最大 VID ---
        long maxVid = getMaxVid(nodeLabel);
         if (maxVid <= 1) {
             System.err.println("Max VID is too small. Cannot run throughput test.");
             return;
         }
        System.out.println("Max VID found: " + maxVid);
        Random rand = new Random();

        // --- 模仿 Nebula Benchmark 逻辑: 预生成并打乱操作列表 ---
        System.out.println("Generating and shuffling operation list...");
        List<Integer> ops = new ArrayList<>((int) MAX_OPS);
        long readOpsCount = (long) (MAX_OPS * readRatio);
        for (long i = 0; i < readOpsCount; i++) {
            ops.add(0); // 0 represents a read operation
        }
        for (long i = 0; i < MAX_OPS - readOpsCount; i++) {
            ops.add(1); // 1 represents a write operation
        }
        Collections.shuffle(ops);
        System.out.println("Operation list generated.");

        try (Session session = driver.session()) {
            long opsCounter = 0;

            for (int op : ops) {
                opsCounter++;

                if (op == 0) { // Read Operation: Get Neighbors
                    long vid = rand.nextLong() % (maxVid - 1) + 1;
                    start = System.nanoTime();
                    session.run("MATCH (a:" + nodeLabel + " {id: $vid})-->(b) RETURN b.id", Map.of("vid", vid));
                    end = System.nanoTime();
                    duration += end - start;
                } else { // Write Operation: Add Edge ONLY
                    // NOTE: The write operation is ONLY Add Edge, to match the Nebula benchmark's logic.
                    // We do not add vertices in this test.
                    long src = rand.nextLong() % (maxVid - 1) + 1;
                    long dst = rand.nextLong() % (maxVid - 1) + 1;
                    start = System.nanoTime();
                    session.run("MATCH (a:" + nodeLabel + " {id: $src}), (b:" + nodeLabel + " {id: $dst}) CREATE (a)-[:REL]->(b)",
                            Map.of("src", src, "dst", dst));
                    end = System.nanoTime();
                    duration += end - start;
                }

                if (opsCounter % PROGRESS_OP_INTERVAL == 0) {
                    double percentage = opsCounter==MAX_BASIC_OP-1 ? 100.0 : (double) (opsCounter+1) / MAX_BASIC_OP * 100;
                    System.out.printf("\rInserting edges: %d / %d (%.2f%%)", opsCounter+1, MAX_BASIC_OP, percentage);
                }
            }

            long totalMillis = TimeUnit.NANOSECONDS.toMillis(duration);
            System.out.printf("Read Write Test Done, Time: %d ms, \n", totalMillis);
            double seconds = duration / 1_000_000_000.0;
            if (seconds > 0) {
                double throughput = MAX_OPS / seconds;
                System.out.printf("Throughput: %.2f op/s\n", throughput);
            } else {
                System.out.println("Throughput: N/A (duration too short to measure)");
            }
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
                long vid = rand.nextLong() % (maxVid - 1) + 1;
                session.run("MATCH (n:" + nodeLabel + " {id: $id}) SET n.newProp = $val", Map.of("id", vid, "val", i));
            }
            long duration = System.nanoTime() - start;
            System.out.printf("Add Vertex Property Latency: %.2f us\n", (double) duration / MAX_BASIC_OP / 1000.0);

            // Delete Vertex Property
            start = System.nanoTime();
            for (int i=0; i < MAX_BASIC_OP; i++) {
                long vid = rand.nextLong() % (maxVid - 1) + 1;
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
