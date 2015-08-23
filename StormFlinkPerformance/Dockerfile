FROM ubuntu:trusty

MAINTAINER pablo
ENV KAFKA_VERSION="0.8.2.1" SCALA_VERSION="2.10"




# Default to UTF-8 file.encoding
ENV LANG C.UTF-8

RUN apt-get update && apt-get install -y unzip openjdk-7-jdk wget curl git docker.io jq


# download Kafka

ADD download-kafka.sh /tmp/download-kafka.sh
RUN sudo chmod -R 3777 /tmp
RUN /tmp/download-kafka.sh
RUN tar xf /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt


VOLUME ["/kafka"]


ENV KAFKA_HOME /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}

RUN $KAFKA_HOME/bin/zookeeper-server-start.sh config/zookeeper.properties &
RUN $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties  &

RUN $KAFKA_HOME/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic neverwinter --partition 1 --replication-factor 1
RUN $KAFKA_HOME/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic random1 --partition 1 --replication-factor 1
RUN $KAFKA_HOME/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic random2 --partition 1 --replication-factor 1
RUN $KAFKA_HOME/bin/kafka-topics.sh  --zookeeper localhost:2181 --create --topic random3 --partition 1 --replication-factor 1


RUN $KAFKA_HOME/bin/kafka-console-consumer.sh --zookeeper localhost:2181 --topic neverwinter


