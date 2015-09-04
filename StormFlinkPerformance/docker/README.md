
Run
---

```bash
docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=192.168.99.100 --env ADVERTISED_PORT=9092 kafkadocker 
```

```bash
export KAFKA=192.168.99.100:9092
$KAFKA_HOME/bin/kafka-console-producer.sh --broker-list $KAFKA --topic test
```

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

java -cp target/flink-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.nventdata.task.flink.FlinkKafkaTopology 192.168.99.100 2181 neverwinter /Users/kidio/message.avsc
```

## storm
```bash
mvn package

java -cp target/storm-app-0.0.1-SNAPSHOT-jar-with-dependencies.jar com.nventdata.task.storm.StormKafkaTopology /Users/kidio/temp/interviewQuestions/StormFlinkPerformance/storm-app/storm-app.properties
```



