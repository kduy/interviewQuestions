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

package org.apache.flink.streaming.connectors.kafka.flink;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.codec.binary.Hex;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.operators.Keys;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitDataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSource;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import kafka.message.Message;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class Main {

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

		//kafkaStream.print();
        
        //kafkaStream.map(new avroDecodingMap()).print();

        SplitDataStream<String> splitStream = kafkaStream.split(new OutputSelector<String>() {
            @Override
            public Iterable<String> select(String value) {
                List<String> outputs = new ArrayList<String>();

                Message message = new Message(value.getBytes());

                ByteBuffer bb = message.payload();

                byte[] avroMessage = new byte[23];
                bb.position(bb.capacity()-23);
                bb.get(avroMessage, 0, avroMessage.length);

                try {
                    Schema _schema = new Schema.Parser().parse(new File("/tmp/message.avsc"));
                    DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(_schema);
                    Decoder decoder = DecoderFactory.get().binaryDecoder(avroMessage, null);
                    GenericRecord result = reader.read(null, decoder);
                    //return new Tuple2<Integer, String>(Integer.parseInt(result.get("random").toString()), Hex.encodeHexString(avroMessage)) ;
                    int randomValue = (Integer)result.get("random");
                    switch (randomValue){
                        case 1:
                            outputs.add("random1");
                            break;
                        case 2:
                            outputs.add("randome2");
                            break;
                        case 3:
                            outputs.add("random3");
                            break;
                        default:
                            
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
                return outputs;
            }
        });

        splitStream.select("random1").print();
		env.execute();
	}


	private static boolean parseParameters(String[] args) {
		if (args.length == 3) {
			host = args[0];
			port = Integer.parseInt(args[1]);
			topic = args[2];
			return true;
		} else {
			System.err.println("Usage: KafkaConsumerExample <host> <port> <topic>");
			return false;
		}
	}
    
    public static final class avroDecodingMap implements MapFunction<String, Tuple2<Integer, String>> {


        @Override
        public Tuple2<Integer,String> map(String value) throws Exception {
            Message message = new Message(value.getBytes());

            ByteBuffer bb = message.payload();

            byte[] avroMessage = new byte[23];
            bb.position(bb.capacity()-23);
            bb.get(avroMessage, 0, avroMessage.length);

            try {
                Schema _schema = new Schema.Parser().parse(new File("/tmp/message.avsc"));
                DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(_schema);
                Decoder decoder = DecoderFactory.get().binaryDecoder(avroMessage, null);
                GenericRecord result = reader.read(null, decoder);
                return new Tuple2<Integer, String>(Integer.parseInt(result.get("random").toString()), Hex.encodeHexString(avroMessage)) ;
            } catch (IOException e) {
                e.printStackTrace();
            }
            return null;
        }
    }

}

 class MySimpleStringSchema implements DeserializationSchema<String> , SerializationSchema<String, Byte[]> {

    @Override
    public String deserialize(byte[] message) {
        return new String(message);
    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public Byte[] serialize(String element) {
        byte[] byteEle =  element.getBytes();
        Byte [] ByteEle = new Byte[byteEle.length];
        for (int i = 0 ; i < byteEle.length; i ++)
            ByteEle[i] = new Byte(byteEle[i]);
        return ByteEle;
    }
    
    public TypeInformation<String> getProducedType (){
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
