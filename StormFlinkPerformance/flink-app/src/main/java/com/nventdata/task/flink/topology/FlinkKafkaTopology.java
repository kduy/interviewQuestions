/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.nventdata.task.flink.topology;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSource;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author pablo
 *
 * The main class to split an avro message stream from kafka source,
 * split based on the `random` field then feed the split stream to kafka sink
 */

public class FlinkKafkaTopology {

    public static Logger LOG = LoggerFactory.getLogger(FlinkKafkaTopology.class);
    public static final  Properties properties = new Properties();

    private static String zkhost;
    private static String brokerList;
    private static String topic;

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment().setParallelism(4);

        DataStream<String> kafkaStream = env
                .addSource(new KafkaSource<String>(zkhost, topic, new AvroSerializationSchema(FlinkKafkaTopology.properties, true)));

        SplitDataStream<String> splitStream = kafkaStream.split(new OutputSelector<String>() {
            @Override
            public Iterable<
                    String> select(String value) {
                List<String> outputs = new ArrayList<String>();
                JSONObject jsonObject = new JSONObject(value.trim());
                int randomField = jsonObject.getInt("random");

                switch (randomField) {
                    case 1:
                        outputs.add("random1");
                        break;
                    case 2:
                        outputs.add("random2");
                        break;
                    case 3:
                        outputs.add("random3");
                        break;
                    default:
                }
                return outputs;
            }
        });
        for (int i = 1; i <=3 ; i++){
            forwardToKafka(splitStream,"random"+i, "random"+i);
        }

        env.execute();

    }

    private static void forwardToKafka(SplitDataStream<String> splitStream,String streamName, String topic) {
        splitStream.select(streamName).addSink(new KafkaSink<String>(brokerList, topic, new AvroSerializationSchema(properties, false)));
    }

    private static boolean parseParameters(String[] args) {
        if (args.length == 1) {
            FileInputStream in = null;
            try {
                in = new FileInputStream(args[0]);
                properties.load(in);
                in.close();
                zkhost = properties.getProperty("zookeeper.hosts");
                brokerList = properties.getProperty("metadata.broker.list");
                topic = properties.getProperty("kafka.topic");

            } catch (FileNotFoundException e) {
                LOG.error("File "+ args[0] + " is not found !");
                e.printStackTrace();
            } catch (IOException e) {
                LOG.error("Cannot load file "+ args[0]);
                e.printStackTrace();
            }

            return true;
        } else {
            System.err.println("Usage: FlinkKafkaTopology <topology property file>");
            return false;
        }
    }
}
