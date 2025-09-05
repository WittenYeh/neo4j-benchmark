import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import java.util.concurrent.Callable;

/**
 * Main entry point for the Neo4j Benchmark Tool.
 * Parses command-line arguments and delegates tasks to the BenchmarkRunner.
 */
@Command(name = "neo4j-benchmark-tool", mixinStandardHelpOptions = true,
        description = "A benchmark tool for Neo4j.")
public class Neo4jBenchmark implements Callable<Integer> {

    // --- Command Line Arguments ---
    @Option(names = "--uri", description = "Neo4j Bolt URI.", defaultValue = "bolt://localhost:7687")
    private String uri;

    @Option(names = "--user", description = "Neo4j username.", defaultValue = "neo4j")
    private String user;

    @Option(names = "--password", description = "Neo4j password.", defaultValue = "password")
    private String password;

    @Option(names = "--graph-name", required = true, description = "The name of the graph to be tested.")
    private String graphName;

    @Option(names = "--data-path", description = "The path of the data file to be loaded.")
    private String dataPath;

    @Option(names = "--command", required = true, description = "The command to execute: ${COMPLETION-CANDIDATES}")
    private CommandType command;

    @Option(names = "--undirected", description = "Specifies if the graph is undirected.")
    private boolean undirected;

    @Option(names = "--read-ratio", description = "The ratio of read operations in the throughput test.", defaultValue = "0.5")
    private double readRatio;

    @Option(names = "--algm", description = "The GDS algorithm to be tested.", defaultValue = "all")
    private String algm;
    
    // --- REFACTORED OPTION ---
    // This option now specifies the path for the clean, final results file.
    // The shell script is responsible for providing this path.
    @Option(names = "--result-path", required = true, description = "Path to save the clean benchmark result file.")
    private String resultPath;

    /**
     * Main execution logic, called by picocli after parsing arguments.
     * @return 0 on success, 1 on failure.
     */
    @Override
    public Integer call() {
        // Pass the resultPath to the BenchmarkRunner.
        try (BenchmarkRunner benchmarker = new BenchmarkRunner(uri, user, password, graphName, resultPath)) {
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
            // Print stack trace to stderr, which will be captured by the log file.
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