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

- create a tocpic
```bash
bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic neverwinter --partition 2 -replication-factor 2
```


- producer
```bash
./bin/kafka-console-producer.sh --broker-list localhost:9092 --topic neverwinter
```

- consumer
```bash
./bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic neverwinter
```