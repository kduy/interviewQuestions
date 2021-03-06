
# HOW TO RUN
---

# Download the repository
```bash
export DOCKER_IP=`docker-machine ip`
git clone https://github.com/kduy/interviewQuestions
cd interviewQuestions/StormFlinkPerformance/
export PROJECT_HOME=$(pwd)
```

# Set up Docker

```bash
#build
docker build -t kafkadocker docker/kafka/

#start
docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=$DOCKER_IP --env ADVERTISED_PORT=9092 --env KAFKA_CREATE_TOPICS=neverwinter,random1,random2,random3 kafkadocker &
```

# Programing Assigment

## Storm
### Build
```bash
cd $PROJECT_HOME/storm-app
mvn clean package
```

### produce data
```bash
python KafkaGenerator.py -k $DOCKER_IP:9092 -m 100000
```

### Process data
```
java -cp target/storm-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.nventdata.task.storm.topology.StormKafkaTopology storm-app.properties
```

`storm-app.properties`  contains information about
- Storm application: such as zookeeper host (`$DOCKER_IP`), kafka topic ...
- Avro schema file path : `avro.schema.filePath` (optional)
- Performance Metric parameters:
    + `performance.name`: name of application
    + `performance.interval`: track metrics every interval
    + `performance.dump.interval`: write metrics to file every interval
    + `performance.dir.output`: output folder contains above file

### Verification
```bash
    java -cp target/storm-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.nventdata.task.storm.verification.KafkaConsumer <zookeeperHost>:<port>  <topics> <timeout> <number of messages>
```
Example:

```bash
    java -cp target/storm-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.nventdata.task.storm.verification.KafkaConsumer $DOCKER_IP:2181 random1,random2,random3 10000 100000
```


**Notice**: `KafkaConsumer` will terminate if no message is arrived before `timeout`. Therefore, make sure that `KafkaGenerator` produces data before that momment. 

If all messages arrive to its correct topics, you should see `All [number of messages] has been arrived !` after `timeout` in Console

### Performance
```bash
java -cp target/storm-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.nventdata.task.storm.performance.Performance <metric file path>
```

`<metric file path>` is the path of file which was generated by the application and stored at `performance.dir.output`

Example:
```bash
java -cp target/storm-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.nventdata.task.storm.performance.Performance /tmp/metrics/storm_81.csv
```

Sample output:
```
    Thoughput in records/s: 1844
    Thoughput in bytes/s: 44459
```

-----------

## Flink
### Build
```bash
cd $PROJECT_HOME/flink-app
mvn clean package
```

### produce data
```bash
python KafkaGenerator.py -k $DOCKER_IP:9092 -m 100000
```

### Process data
```
java -cp target/flink-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.nventdata.task.flink.topology.FlinkKafkaTopology flink-app.properties
```

`flink-app.properties`  contains information about
- Flink application: such as zookeeper host (`$DOCKER_IP`), kafka topic , broker lists
- Avro schema file path : `avro.schema.filePath` (optional)
- Performance Metric parameters:
    + `performance.name`: name of application
    + `performance.interval`: track metrics every interval
    + `performance.dump.interval`: write metrics to file every interval
    + `performance.dir.output`: output folder contains above file

### Verification
```bash
    java -cp target/flink-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.nventdata.task.flink.verification.KafkaConsumer  <zookeeperHost>:<port>  <topics> <timeout> <number of messages>
```
Example:

```bash
    java -cp target/flink-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.nventdata.task.flink.verification.KafkaConsumer $DOCKER_IP:2181 random1,random2,random3 10000 100000
```


**Notice**: `KafkaConsumer` will terminate if no message is arrived before `timeout`. Therefore, make sure that `KafkaGenerator` produces data before that momment. 

If all messages arrive to its correct topics, you should see `All [number of messages] has been arrived !` after `timeout` in Console

### Performance
```bash
java -cp target/flink-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.nventdata.task.flink.performance.Performance <metric file path>
```

`<metric file path>` is the path of file which was generated by the application and stored at `performance.dir.output`

Example:
```bash
java -cp target/flink-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.nventdata.task.flink.performance.Performance /tmp/metrics/flink_34.csv
```

Sample output:
```
    Thoughput in records/s: 1844
    Thoughput in bytes/s: 44459
```