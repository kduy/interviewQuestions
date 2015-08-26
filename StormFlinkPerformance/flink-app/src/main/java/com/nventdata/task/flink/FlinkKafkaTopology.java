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

package com.nventdata.task.flink;

import com.nventdata.task.flink.ex.AvroConsumer;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
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

public class FlinkKafkaTopology {

    private static String host;
    private static int port;
    private static String topic;

    public static void main(String[] args) throws Exception {

        if (!parseParameters(args)) {
            return;
        }

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment().setParallelism(4);

        DataStream<String> kafkaStream = env
                .addSource(new KafkaSource<String>(host + ":" + port, topic, new MySimpleStringSchema()));

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
        splitStream.select(streamName).addSink(new KafkaSink<String>(host + ":" + 9092, topic, new MySimpleStringSchema()));
    }

    private static boolean parseParameters(String[] args) {
        if (args.length == 3) {
            host = args[0];
            port = Integer.parseInt(args[1]);
            topic = args[2];
            return true;
        } else {
            System.err.println("Usage: FlinkKafkaTopology <host> <port> <topic>");
            return false;
        }
    }
}

class MySimpleStringSchema implements SerializationSchema<String, byte[]> , DeserializationSchema<String>  {

    @Override
    public String deserialize(byte[] message) {
        try {
            Schema _schema = new Schema.Parser().parse(new File("/tmp/message.avsc"));
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(_schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(message, null);
            GenericRecord result = reader.read(null, decoder);
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
            return jsonToAvro(element, "/tmp/message.avsc");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return element.getBytes();
    }

    public TypeInformation<String> getProducedType (){
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    public static byte[] jsonToAvro(String json, String filePath) throws IOException {
        InputStream input = null;
        GenericDatumWriter<GenericRecord> writer = null;
        Encoder encoder = null;
        ByteArrayOutputStream output = null;
        try {
            Schema schema = new Schema.Parser().parse(new File(filePath));
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
