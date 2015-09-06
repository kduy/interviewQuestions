
How To Run
---

```bash
expport DOCKER_IP=`docker-machine ip`
git clone https://github.com/kduy/interviewQuestions
cd interviewQuestions/StormFlinkPerformance/
```

# Set up Docker

```bash
#build
docker build -t kafkadocker docker/kafka/

#start
docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=$DOCKER_IP --env ADVERTISED_PORT=9092 kafkadocker 

# create topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic test

```

# Programing Assigment

## Storm


## Flink





```bash
docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=192.168.99.100 --env ADVERTISED_PORT=9092 kafkadocker 
```

```bash
export KAFKA=192.168.99.100:9092
$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list $KAFKA --topic test
```


Consumer 
```bash
export ZOOKEEPER=192.168.99.100:2181
$KAFKA_HOME/bin/kafka-console-consumer.sh --zookeeper $ZOOKEEPER --topic test
```




Build from source
---

    docker build -t kafkaDocker kafka/



# produce
```bash

python KafkaGenerator.py -k 192.168.99.100:9092 -m 1
```




# Run Main Program
## flink 
```bash
mvn package

java -cp target/flink-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.nventdata.task.flink.topology.FlinkKafkaTopology /Users/kidio/temp/interviewQuestions/StormFlinkPerformance/flink-app/flink-app.properties
```

### Verification
```bash
    java -cp target/flink-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.nventdata.task.flink.verification.KafkaConsumer 192.168.99.100:2181 random1,random2,random3 10000 1000
```

## storm
```bash
mvn package

java -cp target/storm-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.nventdata.task.storm.topology.StormKafkaTopology /Users/kidio/temp/interviewQuestions/StormFlinkPerformance/storm-app/storm-app.properties
```


### Verification
```bash
    java -cp target/storm-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.nventdata.task.storm.verification.KafkaConsumer 192.168.99.100:2181 random1,random2,random3 10000 1000
```





