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
mvn clean install -T 1 -DskipTests
cp packaging/standalone/target/neo4j-community-5.26.0-unix.tar.gz ~/neo4j-benchmark/neo4j-compiled/
cd ~/neo4j-benchmark/neo4j-compiled/
tar -xzf neo4j-community-5.26.0-unix.tar.gz
mv neo4j-community-5.26.0 neo4j-server
export NEO4J_HOME=~/neo4j-benchmark/neo4j-compiled/neo4j-server

# install neo4j GDS plugin
wget https://github.com/neo4j/graph-data-science/releases/download/2.13.2/neo4j-graph-data-science-2.13.2.jar
mv neo4j-graph-data-science-2.13.2.jar $NEO4J_HOME/plugins/
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

### Download dataset

| Dataset Name   | Directed   | Property | Nodes      | Edges         |
| -------------- | ---------- | -------- | ---------- | ------------- |
| com-DBLP       | undirected | NO       | 317,080    | 1,049,866     |
| Twitch         | undirected | NO       | 168,114    | 6,797,557     |
| Wiki-Talk      | directed   | NO       | 2,394,385  | 5,021,410     |
| Cit-Patents    | directed   | NO       | 3,774,768  | 16,518,947    |
| Wikipedia      |            | NO       | 3,333,397  | 123,709,902   |
| Orkut          | undirected | NO       | 3,072,441  | 234,370,166   |
| Freebase Large |            | YES      | 28,408,172 | 31,475,362    |
| Twitter        | directed   | NO       | 41,652,230 | 1,202,513,046 |

### Running Tests

launch neo4j server:

```sh
# set your password at first time 
$NEO4J_HOME/bin/neo4j start
$NEO4J_HOME/bin/neo4j status
```

admin login and set passowrd

```sh
$NEO4J_HOME/bin/neo4j stop
export NEO4J_PASSWORD="neo4j-password"
$NEO4J_HOME/bin/neo4j-admin dbms set-initial-password $NEO4J_PASSWORD
$NEO4J_HOME/bin/neo4j start # restart neo4j
```


