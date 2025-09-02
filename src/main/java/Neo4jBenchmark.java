import org.neo4j.driver.*;
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

    // --- Main Execution Logic ---
    @Override
    public Integer call() throws Exception {
        try (BenchmarkRunner benchmarker = new BenchmarkRunner(uri, user, password, graphName)) {
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
                case test_property:  // maybe not vert precise now
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
