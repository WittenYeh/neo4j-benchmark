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

public class BenchmarkRunner implements AutoCloseable {
    private final Driver driver;
    private final String graphName;
    private final ReportManager reportManager;
    private final int BATCH_SIZE = 2048;
    private final long MAX_OPS = 20000; // Total operations for throughput test
    private final int MAX_BASIC_OP = 1000; // Operations per latency test

    private final int PROGRESS_BATCH_INTERVAL = 10;
    private final int PROGRESS_OP_INTERVAL = 100;

    public BenchmarkRunner(String uri, String user, String password, String graphName, String reportPath) {
        this.driver = GraphDatabase.driver(uri, AuthTokens.basic(user, password));
        this.graphName = graphName;
        this.reportManager = new ReportManager(reportPath);
        addReportHeader();
    }

    private void addReportHeader() {
        reportManager.append("=================================================");
        reportManager.append("        Neo4j Benchmark Report");
        reportManager.append("=================================================");
        reportManager.append("Date: " + new java.util.Date());
        reportManager.append("Graph Name: " + this.graphName);
        reportManager.append("Neo4j URI: " + driver.defaultConfig().uri());
        reportManager.append("OS: " + System.getProperty("os.name") + " (" + System.getProperty("os.version") + ")");
        reportManager.append("Java Version: " + System.getProperty("java.version"));
        reportManager.append("-------------------------------------------------");
    }

    @Override
    public void close() throws Exception {
        reportManager.writeReportToFile();
        driver.close();
    }

    // --- Schema and Data Management ---

    public void dropGraph() {
        reportManager.appendSectionHeader("Dropping Graph");
        reportManager.append("Dropping graph '" + graphName + "'...");
        try (Session session = driver.session()) {
            session.run("CALL gds.graph.drop($graphName, false) YIELD graphName", Map.of("graphName", graphName));
            reportManager.append("GDS graph projection dropped (if existed).");
        } catch (Exception e) {
            reportManager.append("Could not drop GDS projection (may not exist).");
        }

        try (Session session = driver.session()) {
            String propertyVLabel = graphName + "_PropertyV";
            session.run("DROP INDEX " + graphName + "_id_index IF EXISTS");
            session.run("DROP INDEX " + propertyVLabel + "_id_index IF EXISTS");
            reportManager.append("Indexes dropped (if existed).");

            reportManager.append("Deleting graph data...");
            String deleteQuery = "MATCH (n) WHERE labels(n)[0] STARTS WITH $graphName CALL { WITH n DETACH DELETE n } IN TRANSACTIONS OF 10000 ROWS";
            session.run(deleteQuery, Map.of("graphName", graphName));
            reportManager.append("Graph data deleted.");
        }
    }

    public void createSchema(boolean isPropertyGraph) {
        reportManager.appendSectionHeader("Creating Schema");
        String nodeLabel = isPropertyGraph ? graphName + "_PropertyV" : graphName;
        try (Session session = driver.session()) {
            session.run("CREATE INDEX " + nodeLabel + "_id_index IF NOT EXISTS FOR (n:" + nodeLabel + ") ON (n.id)");
            reportManager.append("Index on " + nodeLabel + "(id) created.");
        }
        reportManager.append("Waiting 10 seconds for index to be online...");
        try { TimeUnit.SECONDS.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }

    public void loadData(String dataPath, boolean isUndirected) throws IOException {
        reportManager.appendSectionHeader("Loading Graph Data");
        reportManager.append("Loading data from '" + dataPath + "'...");
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
            for (int i = 0; i < nodeList.size(); i += BATCH_SIZE) {
                List<Map<String, Object>> batch = nodeList.subList(i, Math.min(i + BATCH_SIZE, nodeList.size()));
                session.run("UNWIND $nodes AS node_data MERGE (n:" + graphName + " {id: node_data.id})", Map.of("nodes", batch));
                int processed = Math.min(i + BATCH_SIZE, nodeList.size());
                if (processed % (BATCH_SIZE * PROGRESS_BATCH_INTERVAL) == 0 || processed == nodeList.size()) {
                    System.out.printf("\rInserted nodes: %d / %d (%.2f%%)", processed, nodeList.size(), (double) processed / nodeList.size() * 100);
                }
            }
            System.out.println();
            reportManager.append("Inserted/Merged %d nodes.", nodes.size());

            System.out.printf("Loading %d relationships...\n", edges.size());
            for (int i = 0; i < edges.size(); i += BATCH_SIZE) {
                List<Map<String, Object>> batch = edges.subList(i, Math.min(i + BATCH_SIZE, edges.size()));
                session.run("UNWIND $edges AS edge MATCH (src:" + graphName + " {id: edge.src}), (dst:" + graphName + " {id: edge.dst}) MERGE (src)-[:REL]->(dst)", Map.of("edges", batch));
                int processed = Math.min(i + BATCH_SIZE, edges.size());
                if (processed % (BATCH_SIZE * PROGRESS_BATCH_INTERVAL) == 0 || processed == edges.size()) {
                     System.out.printf("\rInserted relationships: %d / %d (%.2f%%)", processed, edges.size(), (double) processed / edges.size() * 100);
                }
            }
            System.out.println();
            reportManager.append("Inserted/Merged %d relationships.", edges.size());
        }

        long endLoadTime = System.currentTimeMillis();
        double durationSeconds = (endLoadTime - startLoadTime) / 1000.0;
        reportManager.append("\n--- Data Loading Summary ---");
        reportManager.append("Total time: %.3f seconds", durationSeconds);
        if (durationSeconds > 0) {
            reportManager.append("Node loading throughput: %.2f nodes/sec", nodes.size() / durationSeconds);
            reportManager.append("Edge loading throughput: %.2f edges/sec", edges.size() / durationSeconds);
        }
    }
    
    public void loadPropertyGraph(String dataPath) throws IOException {
        reportManager.appendSectionHeader("Loading Property Graph Data");
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
            reportManager.append("Loading vertices from %s", vertexFile);
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
                 session.run("UNWIND $batch as props CREATE (n:" + vertexLabel + ") SET n = props", Map.of("batch", batch));
                 int processed = Math.min(i + BATCH_SIZE, vertices.size());
                 if (processed % (BATCH_SIZE * PROGRESS_BATCH_INTERVAL) == 0 || processed == vertices.size()) {
                    System.out.printf("\rInserting vertices: %d / %d (%.2f%%)", processed, vertices.size(), (double) processed / vertices.size() * 100);
                 }
             }
             System.out.println();
            reportManager.append("Loaded %d vertices.", vertices.size());

            reportManager.append("Loading edges from %s", edgeFile);
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
                            "MATCH (src:" + vertexLabel + " {id: edge.src}) " +
                            "MATCH (dst:" + vertexLabel + " {id: edge.dst}) " +
                            "CREATE (src)-[r:" + edgeType + "]->(dst) SET r = edge.props", Map.of("batch", batch));
                int processed = Math.min(i + BATCH_SIZE, edges.size());
                if (processed % (BATCH_SIZE * PROGRESS_BATCH_INTERVAL) == 0 || processed == edges.size()) {
                    System.out.printf("\rInserting edges: %d / %d (%.2f%%)", processed, edges.size(), (double) processed / edges.size() * 100);
                }
            }
            System.out.println();
            reportManager.append("Loaded %d edges.", edges.size());
        }

        long endLoadTime = System.currentTimeMillis();
        double durationSeconds = (endLoadTime - startLoadTime) / 1000.0;
        reportManager.append("\n--- Property Graph Loading Summary ---");
        reportManager.append("Total time: %.3f seconds", durationSeconds);
        if (durationSeconds > 0) {
            reportManager.append("Vertex loading throughput: %.2f vertices/sec", vertices.size() / durationSeconds);
            reportManager.append("Edge loading throughput: %.2f edges/sec", edges.size() / durationSeconds);
        }
    }

    private long getMaxVid(String nodeLabel) {
        try (Session session = driver.session()) {
            Result result = session.run("MATCH (n:" + nodeLabel + ") RETURN max(n.id) AS maxId");
            if (result.hasNext()) {
                Value maxIdValue = result.single().get("maxId");
                if (!maxIdValue.isNull()) return maxIdValue.asLong();
            }
            return 0L;
        }
    }

    public void basicOpTest() {
        reportManager.appendSectionHeader("Basic Operations Latency Test");
        String nodeLabel = graphName;
        long maxVid = getMaxVid(nodeLabel);
        if (maxVid <= 1) {
            System.err.println("Max VID is too small. Cannot run basic op test. Is the graph loaded correctly?");
            reportManager.append("ERROR: Max VID is too small. Cannot run basic op test.");
            return;
        }
        Random rand = new Random();
        List<Long> latencies = new ArrayList<>(MAX_BASIC_OP);
        long start, end;

        try (Session session = driver.session()) {
            // 1. Add Vertex
            for (int i = 0; i < MAX_BASIC_OP; i++) {
                long vid = maxVid + i + 1;
                start = System.nanoTime();
                session.run("CREATE (n:" + nodeLabel + " {id: $id})", Map.of("id", vid));
                end = System.nanoTime();
                latencies.add(end - start);
                if ((i+1) % PROGRESS_OP_INTERVAL == 0 || (i+1) == MAX_BASIC_OP) System.out.printf("\rAdd Vertex: %d / %d", i+1, MAX_BASIC_OP);
            }
            System.out.println();
            reportManager.append("\n--- Operation: Add Vertex ---");
            reportManager.append("Total Operations: %d", MAX_BASIC_OP);
            reportManager.append(LatencyStats.fromNanos(latencies).toString());
            latencies.clear();

            // 2. Add Edge
            for (int i = 0; i < MAX_BASIC_OP; i++) {
                long src = rand.nextLong() % (maxVid - 1) + 1;
                long dst = rand.nextLong() % (maxVid - 1) + 1;
                start = System.nanoTime();
                session.run("MATCH (a:" + nodeLabel + " {id: $src}), (b:" + nodeLabel + " {id: $dst}) CREATE (a)-[:REL]->(b)", Map.of("src", src, "dst", dst));
                end = System.nanoTime();
                latencies.add(end - start);
                if ((i+1) % PROGRESS_OP_INTERVAL == 0 || (i+1) == MAX_BASIC_OP) System.out.printf("\rAdd Edge: %d / %d", i+1, MAX_BASIC_OP);
            }
            System.out.println();
            reportManager.append("\n--- Operation: Add Edge ---");
            reportManager.append("Total Operations: %d", MAX_BASIC_OP);
            reportManager.append(LatencyStats.fromNanos(latencies).toString());
            latencies.clear();
            
            // 3. Delete Edge
            long successfulDeletions = 0;
            for (int i = 0; i < MAX_BASIC_OP; i++) {
                long src = rand.nextLong() % (maxVid - 1) + 1;
                Result findNeighborResult = session.run("MATCH (a:" + nodeLabel + " {id: $src})-[:REL]->(b) RETURN b.id AS dstId LIMIT 1", Map.of("src", src));
                if (findNeighborResult.hasNext()) {
                    long dstId = findNeighborResult.single().get("dstId").asLong();
                    start = System.nanoTime();
                    session.run("MATCH (a:" + nodeLabel + " {id: $src})-[r:REL]->(b:" + nodeLabel + " {id: $dst}) WITH r LIMIT 1 DELETE r", Map.of("src", src, "dst", dstId));
                    end = System.nanoTime();
                    latencies.add(end - start);
                    successfulDeletions++;
                }
                if ((i+1) % PROGRESS_OP_INTERVAL == 0 || (i+1) == MAX_BASIC_OP) System.out.printf("\rDelete Edge: %d / %d", i+1, MAX_BASIC_OP);
            }
            System.out.println();
            reportManager.append("\n--- Operation: Delete Edge ---");
            reportManager.append("Attempted Operations: %d", MAX_BASIC_OP);
            reportManager.append("Successful Deletions: %d", successfulDeletions);
            reportManager.append(LatencyStats.fromNanos(latencies).toString());
            latencies.clear();

            // 4. Get Neighbors
            for (int i = 0; i < MAX_BASIC_OP; i++) {
                long vid = rand.nextLong() % (maxVid - 1) + 1;
                start = System.nanoTime();
                session.run("MATCH (a:" + nodeLabel + " {id: $vid})-->(b) RETURN b.id", Map.of("vid", vid));
                end = System.nanoTime();
                latencies.add(end - start);
                if ((i+1) % PROGRESS_OP_INTERVAL == 0 || (i+1) == MAX_BASIC_OP) System.out.printf("\rGet Neighbors: %d / %d", i+1, MAX_BASIC_OP);
            }
            System.out.println();
            reportManager.append("\n--- Operation: Get Neighbors ---");
            reportManager.append("Total Operations: %d", MAX_BASIC_OP);
            reportManager.append(LatencyStats.fromNanos(latencies).toString());
        }
    }
    
    public void readWriteTest(double readRatio) {
        reportManager.appendSectionHeader("Read/Write Throughput Test");
        reportManager.append("Configuration: Total Ops=%d, Read Ratio=%.2f", MAX_OPS, readRatio);
        String nodeLabel = graphName;
        long duration = 0;

        long maxVid = getMaxVid(nodeLabel);
         if (maxVid <= 1) {
             System.err.println("Max VID is too small. Cannot run throughput test.");
             reportManager.append("ERROR: Max VID is too small. Cannot run test.");
             return;
         }
        reportManager.append("Max VID found: %d", maxVid);
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

            for (int op : ops) {
                if (op == 0) { // Read
                    long vid = rand.nextLong() % (maxVid - 1) + 1;
                    session.run("MATCH (a:" + nodeLabel + " {id: $vid})-->(b) RETURN b.id", Map.of("vid", vid));
                } else { // Write
                    long src = rand.nextLong() % (maxVid - 1) + 1;
                    long dst = rand.nextLong() % (maxVid - 1) + 1;
                    session.run("MATCH (a:" + nodeLabel + " {id: $src}), (b:" + nodeLabel + " {id: $dst}) CREATE (a)-[:REL]->(b)", Map.of("src", src, "dst", dst));
                }
                opsCounter++;
                if (opsCounter % (PROGRESS_OP_INTERVAL * 10) == 0 || opsCounter == MAX_OPS) {
                    System.out.printf("\rExecuting Ops: %d / %d (%.2f%%)", opsCounter, MAX_OPS, (double) opsCounter / MAX_OPS * 100);
                }
            }
            duration = System.nanoTime() - testStartNanos;
            System.out.println();
        }

        double seconds = duration / 1_000_000_000.0;
        reportManager.append("\n--- Throughput Test Summary ---");
        reportManager.append("Total operations executed: %d", MAX_OPS);
        reportManager.append("Total time: %.3f seconds", seconds);
        if (seconds > 0) {
            reportManager.append("Throughput: %.2f op/s", MAX_OPS / seconds);
        } else {
            reportManager.append("Throughput: N/A (duration too short to measure)");
        }
    }
    
    public void propertyOpTest() {
        reportManager.appendSectionHeader("Property Operations Latency Test");
        String nodeLabel = graphName + "_PropertyV";
        long maxVid = getMaxVid(nodeLabel);
        if (maxVid <= 1) {
             System.err.println("Max VID is too small. Cannot run property test.");
             reportManager.append("ERROR: Max VID is too small. Is the property graph loaded?");
             return;
        }
        Random rand = new Random();
        List<Long> latencies = new ArrayList<>(MAX_BASIC_OP);
        long start, end;
        
        try (Session session = driver.session()) {
            // Add Vertex Property
            for (int i=0; i < MAX_BASIC_OP; i++) {
                long vid = rand.nextLong() % (maxVid - 1) + 1;
                start = System.nanoTime();
                session.run("MATCH (n:" + nodeLabel + " {id: $id}) SET n.newProp = $val", Map.of("id", vid, "val", i));
                end = System.nanoTime();
                latencies.add(end - start);
                if ((i+1) % PROGRESS_OP_INTERVAL == 0 || (i+1) == MAX_BASIC_OP) System.out.printf("\rAdd Property: %d / %d", i+1, MAX_BASIC_OP);
            }
            System.out.println();
            reportManager.append("\n--- Operation: Add Vertex Property ---");
            reportManager.append("Total Operations: %d", MAX_BASIC_OP);
            reportManager.append(LatencyStats.fromNanos(latencies).toString());
            latencies.clear();

            // Delete Vertex Property
            for (int i=0; i < MAX_BASIC_OP; i++) {
                long vid = rand.nextLong() % (maxVid - 1) + 1;
                start = System.nanoTime();
                session.run("MATCH (n:" + nodeLabel + " {id: $id}) REMOVE n.newProp", Map.of("id", vid));
                end = System.nanoTime();
                latencies.add(end - start);
                if ((i+1) % PROGRESS_OP_INTERVAL == 0 || (i+1) == MAX_BASIC_OP) System.out.printf("\rDelete Property: %d / %d", i+1, MAX_BASIC_OP);
            }
            System.out.println();
            reportManager.append("\n--- Operation: Delete Vertex Property ---");
            reportManager.append("Total Operations: %d", MAX_BASIC_OP);
            reportManager.append(LatencyStats.fromNanos(latencies).toString());
        }
    }

    public void testAlgm(String algm) {
        reportManager.appendSectionHeader("GDS Algorithm Test");
        reportManager.append("Testing algorithm(s): " + algm);
        String nodeLabel = graphName;
        String relType = "REL";

        try (Session session = driver.session()) {
            reportManager.append("Projecting graph to GDS...");
            session.run("CALL gds.graph.project($graphName, $nodeLabel, $relType)",
                    Map.of("graphName", graphName, "nodeLabel", nodeLabel, "relType", relType));
            
            if ("all".equals(algm) || "pagerank".equals(algm)) {
                long start = System.currentTimeMillis();
                session.run("CALL gds.pageRank.stream($graphName) YIELD nodeId, score RETURN count(*)", Map.of("graphName", graphName));
                reportManager.append("PageRank Done, Time: %.3f s", (System.currentTimeMillis() - start) / 1000.0);
            }
            if ("all".equals(algm) || "wcc".equals(algm)) {
                long start = System.currentTimeMillis();
                session.run("CALL gds.wcc.stream($graphName) YIELD nodeId, componentId RETURN count(*)", Map.of("graphName", graphName));
                reportManager.append("WCC Done, Time: %.3f s", (System.currentTimeMillis() - start) / 1000.0);
            }
            if ("all".equals(algm) || "cdlp".equals(algm)) {
                long start = System.currentTimeMillis();
                session.run("CALL gds.labelPropagation.stream($graphName) YIELD nodeId, communityId RETURN count(*)", Map.of("graphName", graphName));
                reportManager.append("CDLP Done, Time: %.3f s", (System.currentTimeMillis() - start) / 1000.0);
            }
            if ("all".equals(algm) || "sssp".equals(algm)) {
                long startVid = graphName.equalsIgnoreCase("Wiki-Talk") ? 1 : 1;
                long start = System.currentTimeMillis();
                session.run("MATCH (start:" + nodeLabel + " {id: $startId}) " +
                            "CALL gds.sssp.stream($graphName, { sourceNode: start }) YIELD nodeId, distance RETURN count(*)",
                        Map.of("graphName", graphName, "startId", startVid));
                reportManager.append("SSSP (startNode=%d) Done, Time: %.3f s", startVid, (System.currentTimeMillis() - start) / 1000.0);
            }
             if ("all".equals(algm) || "bfs".equals(algm)) {
                long startVid = graphName.equalsIgnoreCase("Wiki-Talk") ? 1 : 1;
                long start = System.currentTimeMillis();
                session.run("MATCH (start:" + nodeLabel + " {id: $startId}) " +
                            "CALL gds.bfs.stream($graphName, { sourceNode: start }) YIELD path RETURN count(*)",
                        Map.of("graphName", graphName, "startId", startVid));
                reportManager.append("BFS (startNode=%d) Done, Time: %.3f s", startVid, (System.currentTimeMillis() - start) / 1000.0);
            }

            reportManager.append("Cleaning up GDS projection...");
            session.run("CALL gds.graph.drop($graphName, false)", Map.of("graphName", graphName));
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
                "Avg Latency: %.2f us\n" +
                "Min Latency: %.2f us\n" +
                "Max Latency: %.2f us\n" +
                "p50 Latency: %.2f us\n" +
                "p95 Latency: %.2f us\n" +
                "p99 Latency: %.2f us",
                avg_us, min_us, max_us, p50_us, p95_us, p99_us
            );
        }
    }

    private static class ReportManager {
        private final String reportPath;
        private final StringBuilder reportBuilder = new StringBuilder();

        public ReportManager(String reportPath) {
            this.reportPath = reportPath;
        }

        public void append(String format, Object... args) {
            String line = String.format(Locale.US, format, args);
            System.out.println(line);
            if (reportPath != null) {
                reportBuilder.append(line).append(System.lineSeparator());
            }
        }

        public void appendSectionHeader(String header) {
            append("\n" + "=".repeat(20) + " " + header + " " + "=".repeat(20));
        }

        public void writeReportToFile() throws IOException {
            if (reportPath != null && reportBuilder.length() > 0) {
                append("\n--- End of Report ---");
                try {
                    Files.write(Paths.get(reportPath), reportBuilder.toString().getBytes());
                    System.out.println("\nBenchmark report saved to: " + reportPath);
                } catch (IOException e) {
                    System.err.println("Failed to write report to file: " + reportPath);
                    throw e;
                }
            }
        }
    }
}