FROM UBUNTU:trusty

MAINTAINER pablo
ENV KAFKA_VERSION="0.8.2.1" SCALA_VERSION="2.10"


RUN apt-get update && apt-get install -y unzip && rm -rf /var/lib/apt/lists/*
wget curl git docker.io jq


# Default to UTF-8 file.encoding
ENV LANG C.UTF-8

ENV JAVA_VERSION 7u79
ENV JAVA_DEBIAN_VERSION 7u79-2.5.6-1~deb8u1

RUN apt-get update && apt-get install -y openjdk-7-jdk="$JAVA_DEBIAN_VERSION" && rm -rf /var/lib/apt/lists/*

# download Kafka
mirror=$(curl --stderr /dev/null https://www.apache.org/dyn/closer.cgi\?as_json\=1 | sed -rn 's/.*"preferred":.*"(.*)"/\1/p')
url="${mirror}kafka/${KAFKA_VERSION}/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
wget -q "${url}" -O "/tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"


RUN tar xf /tmp/kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz -C /opt


VOLUME ["/kafka"]

ENV KAFKA_HOME /opt/kafka_${SCALA_VERSION}-${KAFKA_VERSION}

CMD "${KAFKA_HOME}/bin/zookeeper-server-start.sh config/zookeeper.properties &"

