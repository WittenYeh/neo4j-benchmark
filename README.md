# neo4j-benchmark

Benchmark neo4j performance

## Guidance

### Preparation

`java-17` and `maven` installation (with conda)

```sh
conda create -n 'neo4j-benchmark' openjdk=17 maven -y
conda activate 'neo4j-benchmark'
```

### Neo4j Installation and Configuration

neo4j installation (with github source code):

```sh
# install neo4j
mkdir ~/neo4j-benchmark/neo4j-compiled
cd ~/neo4j-benchmark/neo4j
mvn clean install -T 1C -DskipTests
cp packaging/standalone/target/neo4j-community-5.26.0-unix.tar.gz ~/neo4j-benchmark/neo4j-compiled/
cd ~/neo4j-benchmark/neo4j-compiled/
tar -xzf neo4j-community-5.26.0-unix.tar.gz
mv neo4j-community-5.26.0 neo4j-server
export NEO4J_HOME=~/neo4j-benchmark/neo4j-compiled/neo4j-server

# install neo4j GDS plugin
wget https://github.com/neo4j/graph-data-science/releases/download/2.20.0/neo4j-graph-data-science-2.20.0.jar
mv neo4j-graph-data-science-2.20.0.jar $NEO4J_HOME/plugins/
```

configurate neo4j server:

```sh
vim ~/neo4j-benchmark/neo4j-compiled/neo4j-server/conf/neo4j.conf
# uncomment the following line:
# server.bolt.listen_address=:7687
```

### Neo4j-benchmark Installation

```sh
cd ~/neo4j-benchmark/
mvn clean package
```


### Running Tests

launch neo4j server:

```sh
# set your password at first time 
$NEO4J_HOME/bin/neo4j start
$NEO4J_HOME/bin/neo4j status
$NEO4J_HOME/bin/neo4j stop
```



