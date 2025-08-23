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

    @Option(names = "--read-ratio", description = "The ratio of read operations in read_write_test.", defaultValue = "0.5")
    private double readRatio;

    @Option(names = "--algm", description = "The algorithm to be tested.", defaultValue = "all")
    private String algm;

    private enum CommandType { load, test, basic, load_property, test_property, test_algm }

    // --- Benchmarker Class ---
    private static class Neo4jBenchmarker implements AutoCloseable {
        private final Driver driver;
        private final String graphName;
        private final int BATCH_SIZE = 512;
        private final int MAX_OPS = 2000000;
        private final int MAX_BASIC_OP = 1000;

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
                
                // Drop data
                session.run("MATCH (n:" + graphName + ") DETACH DELETE n");
                session.run("MATCH (n:" + propertyVLabel + ") DETACH DELETE n");
                System.out.println("Graph data deleted.");
            }
        }

        public void createSchema(boolean isPropertyGraph) {
            System.out.println("Creating schema (indexes)...");
            String nodeLabel = isPropertyGraph ? graphName + "_PropertyV" : graphName;
            try (Session session = driver.session()) {
                session.run("CREATE INDEX " + nodeLabel + "_id_index IF NOT EXISTS FOR (n:" + nodeLabel + ") ON (n.id)");
                System.out.println("Index on " + nodeLabel + "(id) created.");
                if (isPropertyGraph) {
                    // Add indexes for property search tests
                    if (graphName.equals("freebase_large")) {
                        session.run("CREATE INDEX freebase_freebaseid_index IF NOT EXISTS FOR (n:" + nodeLabel + ") ON (n.freebaseid)");
                    } else if (graphName.equals("ldbc")) {
                        session.run("CREATE INDEX ldbc_xlabel_index IF NOT EXISTS FOR (n:" + nodeLabel + ") ON (n.xlabel)");
                    }
                }
            }
            // Wait for index to come online
            try { TimeUnit.SECONDS.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }

        public void loadData(String dataPath, boolean isUndirected) throws IOException {
             System.out.println("Loading data from '" + dataPath + "'...");
             Set<Long> nodes = new HashSet<>();
             List<Map<String, Object>> edges = new ArrayList<>();

             try (BufferedReader reader = new BufferedReader(new FileReader(dataPath))) {
                 String line;
                 while ((line = reader.readLine()) != null) {
                     String[] parts = line.trim().split("\\s+");
                     if (parts.length != 2) continue;
                     long src = Long.parseLong(parts[0]);
                     long dst = Long.parseLong(parts[1]);
                     nodes.add(src);
                     nodes.add(dst);
                     edges.add(Map.of("src", src, "dst", dst));
                     if (isUndirected) {
                         edges.add(Map.of("src", dst, "dst", src));
                     }
                 }
             }

             List<Map<String, Object>> nodeList = nodes.stream().map(n -> Map.of("id", (Object)n)).collect(Collectors.toList());

             try (Session session = driver.session()) {
                 // Batch insert nodes
                 for (int i = 0; i < nodeList.size(); i += BATCH_SIZE) {
                     List<Map<String, Object>> batch = nodeList.subList(i, Math.min(i + BATCH_SIZE, nodeList.size()));
                     session.run("UNWIND $nodes AS node_data MERGE (n:" + graphName + " {id: node_data.id})", Map.of("nodes", batch));
                 }
                 System.out.printf("Inserted/Merged %d nodes.\n", nodes.size());

                 // Batch insert relationships
                 for (int i = 0; i < edges.size(); i += BATCH_SIZE) {
                     List<Map<String, Object>> batch = edges.subList(i, Math.min(i + BATCH_SIZE, edges.size()));
                     session.run("UNWIND $edges AS edge " +
                                 "MATCH (src:" + graphName + " {id: edge.src}) " +
                                 "MATCH (dst:" + graphName + " {id: edge.dst}) " +
                                 "MERGE (src)-[:REL]->(dst)", Map.of("edges", batch));
                 }
                 System.out.printf("Inserted/Merged %d relationships.\n", edges.size());
             }
        }

        public void loadPropertyGraph(String dataPath) throws IOException {
            String vertexLabel = graphName + "_PropertyV";
            String edgeType = graphName + "_PropertyE";
            Path vertexFile = Paths.get(dataPath + ".vertex");
            Path edgeFile = Paths.get(dataPath + ".edge");

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
                         props.put(kv[0], kv[1]);
                     }
                     vertices.add(props);
                }
                 for (int i = 0; i < vertices.size(); i += BATCH_SIZE) {
                     List<Map<String, Object>> batch = vertices.subList(i, Math.min(i + BATCH_SIZE, vertices.size()));
                     session.run("UNWIND $batch as props CREATE (n:" + vertexLabel + ") SET n = props", Map.of("batch", batch));
                 }
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
                          props.put(kv[0], kv[1]);
                     }
                     edgeData.put("props", props);
                     edges.add(edgeData);
                 }
                 for (int i = 0; i < edges.size(); i += BATCH_SIZE) {
                     List<Map<String, Object>> batch = edges.subList(i, Math.min(i + BATCH_SIZE, edges.size()));
                     session.run("UNWIND $batch as edge " +
                                 "MATCH (src:" + vertexLabel + " {id: edge.src}) " +
                                 "MATCH (dst:" + vertexLabel + " {id: edge.dst}) " +
                                 "CREATE (src)-[r:" + edgeType + "]->(dst) SET r = edge.props", Map.of("batch", batch));
                 }
                System.out.printf("Loaded %d edges.\n", edges.size());
            }
        }
        
        // --- Test Execution ---
        
        private long getMaxVid(String nodeLabel) {
            try (Session session = driver.session()) {
                Result result = session.run("MATCH (n:" + nodeLabel + ") RETURN max(n.id) AS maxId");
                return result.single().get("maxId", 0L);
            }
        }

        public void basicOpTest() {
             System.out.println("Running basic operations test...");
             long maxVid = getMaxVid(graphName);
             if (maxVid == 0) {
                 System.err.println("Max VID is 0. Cannot run basic op test. Is the graph loaded?");
                 return;
             }
             Random rand = new Random();
             
             try (Session session = driver.session()) {
                 // 1. Add Vertex
                 long start = System.nanoTime();
                 for (int i = 0; i < MAX_BASIC_OP; i++) {
                     long vid = rand.nextInt((int) maxVid) + 1;
                     session.run("CREATE (:" + graphName + " {id: " + vid + "})");
                 }
                 long duration = System.nanoTime() - start;
                 System.out.printf("Add Vertex Done, Time per op: %.2f ns\n", (double) duration / MAX_BASIC_OP);

                 // 2. Add Edge
                 start = System.nanoTime();
                 for (int i = 0; i < MAX_BASIC_OP; i++) {
                     long src = rand.nextInt((int) maxVid) + 1;
                     long dst = rand.nextInt((int) maxVid) + 1;
                     session.run("MATCH (a:" + graphName + " {id: " + src + "}), (b:" + graphName + " {id: " + dst + "}) CREATE (a)-[:REL]->(b)");
                 }
                 duration = System.nanoTime() - start;
                 System.out.printf("Add Edge Done, Time per op: %.2f ns\n", (double) duration / MAX_BASIC_OP);
                 
                 // 3. Get Neighbors
                 start = System.nanoTime();
                 for (int i = 0; i < MAX_BASIC_OP; i++) {
                      long vid = rand.nextInt((int) maxVid) + 1;
                      session.run("MATCH (a:" + graphName + " {id: " + vid + "})-->(b) RETURN b.id");
                 }
                 duration = System.nanoTime() - start;
                 System.out.printf("Get Neighbors Done, Time per op: %.2f ns\n", (double) duration / MAX_BASIC_OP);
             }
        }

        public void testAlgm(String algm) {
            System.out.println("Testing algorithm: " + algm);
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
                    System.out.printf("PageRank Done, Time: %d ms\n", System.currentTimeMillis() - start);
                }
                if ("all".equals(algm) || "wcc".equals(algm)) {
                    long start = System.currentTimeMillis();
                    session.run("CALL gds.wcc.stream($graphName) YIELD nodeId, componentId RETURN count(*)", Map.of("graphName", graphName));
                    System.out.printf("WCC Done, Time: %d ms\n", System.currentTimeMillis() - start);
                }
                if ("all".equals(algm) || "cdlp".equals(algm)) {
                    long start = System.currentTimeMillis();
                    session.run("CALL gds.labelPropagation.stream($graphName, { maxIterations: 10 }) YIELD nodeId, communityId RETURN count(*)", Map.of("graphName", graphName));
                    System.out.printf("CDLP Done, Time: %d ms\n", System.currentTimeMillis() - start);
                }
                if ("all".equals(algm) || "sssp".equals(algm)) {
                    long startVid = "wikitalk".equals(graphName) ? 32822L : 3494505L;
                    long endVid = "wikitalk".equals(graphName) ? 33L : 754148L;
                    long start = System.currentTimeMillis();
                    session.run("MATCH (start:" + nodeLabel + " {id: $startId}), (end:" + nodeLabel + " {id: $endId}) " +
                                "CALL gds.shortestPath.dijkstra.stream($graphName, { sourceNode: id(start), targetNode: id(end) }) YIELD path RETURN path",
                            Map.of("graphName", graphName, "startId", startVid, "endId", endVid));
                    System.out.printf("SSSP Done, Time: %d ms\n", System.currentTimeMillis() - start);
                }
                 if ("all".equals(algm) || "bfs".equals(algm)) {
                    long startVid = "wikitalk".equals(graphName) ? 6765L : 3494505L;
                    long start = System.currentTimeMillis();
                    session.run("MATCH (start:" + nodeLabel + " {id: $startId}) " +
                                "CALL gds.bfs.stream($graphName, { sourceNode: id(start), maxDepth: 5 }) YIELD path RETURN count(*)",
                            Map.of("graphName", graphName, "startId", startVid));
                    System.out.printf("BFS Done, Time: %d ms\n", System.currentTimeMillis() - start);
                }
            }
        }
    }

    // --- Main Execution Logic ---
    @Override
    public Integer call() throws Exception {
        try (Neo4jBenchmarker benchmarker = new Neo4jBenchmarker(uri, user, password, graphName)) {
            switch (command) {
                case load:
                    benchmarker.dropGraph();
                    benchmarker.createSchema(false);
                    benchmarker.loadData(dataPath, undirected);
                    break;
                case load_property:
                    benchmarker.dropGraph();
                    benchmarker.createSchema(true);
                    benchmarker.loadPropertyGraph(dataPath);
                    break;
                case basic:
                    benchmarker.basicOpTest();
                    break;
                case test_algm:
                    benchmarker.testAlgm(algm);
                    break;
                // Add cases for 'test' and 'test_property' if needed
                default:
                    System.err.println("Command not yet implemented in this Java version: " + command);
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
