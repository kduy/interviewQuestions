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
package ex;
import com.nventdata.task.flink.performance.Performance;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSink;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSource;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.json.JSONObject;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class FlinkKafkaTopology {


    static final String AVRO_MSG_SCHEMA_FILE_PATH = "src/main/resources/message.avsc";


    private static String zkhost;
    private static String brokerList;
    private static String topic;

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment().setParallelism(4);

        DataStream<String> kafkaStream = env
                .addSource(new KafkaSource<String>(zkhost, topic, new MySimpleStringSchema(AVRO_MSG_SCHEMA_FILE_PATH)));

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
        splitStream.select(streamName).addSink(new KafkaSink<String>(brokerList, topic, new MySimpleStringSchema(AVRO_MSG_SCHEMA_FILE_PATH)));
    }

    private static boolean parseParameters(String[] args) {
        if (args.length == 1) {
            Properties properties = new Properties();
            FileInputStream in = null;
            try {
                in = new FileInputStream(args[0]);
                properties.load(in);
                in.close();
                zkhost = properties.getProperty("zookeeper.hosts");
                brokerList = properties.getProperty("metadata.broker.list");
                topic = properties.getProperty("kafka.topic");

            } catch (FileNotFoundException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            }

            return true;
        } else {
            System.err.println("Usage: FlinkKafkaTopology <topology property file>");
            return false;
        }
    }
}

class MySimpleStringSchema implements SerializationSchema<String, byte[]>, DeserializationSchema<String> {

    static  final Performance perf = new Performance("flink", 100, 1000, "flink");

    private String schemaStr;
    /*static final String schemaStr = "{" +
            " \"name\": \"kafkatest\"," +
            " \"namespace\": \"test\"," +
            " \"type\": \"record\"," +
            " \"fields\": [" +
            " {\"name\": \"id\", \"type\": \"int\"}," +
            " {\"name\": \"random\", \"type\": \"int\"}," +
            " {\"name\": \"data\", \"type\": \"string\"}" +
            " ]" +
            " }";
    */
    public MySimpleStringSchema (String schemaFilePath) {

        try {
            BufferedReader br = new BufferedReader(new FileReader(schemaFilePath));
            String line ;
            StringBuilder sb = new StringBuilder();
            while ((line = br.readLine())!=null){
                sb.append(line);
            }
            schemaStr = sb.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public String deserialize(byte[] message) {
        try {
            Schema _schema = new Schema.Parser().parse(schemaStr);
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(_schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(message, null);
            GenericRecord result = reader.read(null, decoder);

            perf.track(1, message.length);
            
            return result.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new String(message);
    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(String element) {
        //return  element.getBytes();
        try {
            return jsonToAvro(element, schemaStr);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return element.getBytes();
    }

    public TypeInformation<String> getProducedType (){
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    public static byte[] jsonToAvro(String json, String schemaStr) throws IOException {
        InputStream input = null;
        GenericDatumWriter<GenericRecord> writer = null;
        Encoder encoder = null;
        ByteArrayOutputStream output = null;
        try {
            Schema schema = new Schema.Parser().parse(schemaStr);
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
            input = new ByteArrayInputStream(json.getBytes());
            output = new ByteArrayOutputStream();
            DataInputStream din = new DataInputStream(input);
            writer = new GenericDatumWriter<GenericRecord>(schema);
            Decoder decoder = DecoderFactory.get().jsonDecoder(schema, din);
            encoder = EncoderFactory.get().binaryEncoder(output, null);
            GenericRecord datum;
            while (true) {
                try {
                    datum = reader.read(null, decoder);
                } catch (EOFException eofe) {
                    break;
                }
                writer.write(datum, encoder);
            }
            encoder.flush();
            return output.toByteArray();
        } finally {
            try { input.close(); } catch (Exception e) { }
        }
    }
}