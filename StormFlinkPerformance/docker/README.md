
Run
---

```bash
docker run -p 2181:2181 -p 9092:9092 --env ADVERTISED_HOST=`boot2docker ip` --env ADVERTISED_PORT=9092 kafkaDocker
```

```bash
export KAFKA=`boot2docker ip`:9092
kafka-console-producer.sh --broker-list $KAFKA --topic test
```

```bash
export ZOOKEEPER=`boot2docker ip`:2181
kafka-console-consumer.sh --zookeeper $ZOOKEEPER --topic test
```

Build from source
---

    docker build -t kafkaDocker kafka/