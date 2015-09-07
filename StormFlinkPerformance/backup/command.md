## Kafka
### start zookeeper server
```bash
./bin/zookeeper-server-start.sh config/zookeeper.properties &
```

### start kafka server
// modify brokerid , port, log director in __server.property__
```bash
./bin/kafka-server-start.sh config/server.properties &
./bin/kafka-server-start.sh config/server2.properties &  
```

### create a tocpic, producer, consumer
```bash
bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic neverwinter --partition 2 --replication-factor 2
```


- producer
```bash
./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic neverwinter
```

- consumer
```bash
./bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic neverwinter
```




----------

```bash
./bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic storm-sentence1

./bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic storm-word

bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic storm-word --partition 2 --replication-factor 2


.//bin/kafka-console-producer.sh --topic=storm-sentence --broker-list=localhost:9092





```
