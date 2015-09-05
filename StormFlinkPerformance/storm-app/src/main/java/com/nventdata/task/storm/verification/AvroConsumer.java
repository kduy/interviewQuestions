package com.nventdata.task.storm.verification;


import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.DefaultDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;


public class AvroConsumer  implements Runnable{

    public static Logger LOG = LoggerFactory.getLogger(AvroConsumer.class);
    private Properties kafkaProps = new Properties();
    private ConsumerConnector consumer;
    private ConsumerConfig config;
    private KafkaStream<byte[], byte[]> stream;
    private String waitTime;
    private int count;
    private String topic;
    private Schema _schema ;



    public static int total = 0;

    public AvroConsumer(String topic){


        this.topic = topic;
        count = 0;
        try {
            _schema = new Schema.Parser().parse(new File("src/main/resources/message.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main (String [] args){
        Thread t1 = (new Thread(new AvroConsumer("random1")));
        Thread t2 = (new Thread(new AvroConsumer("random2")));
        Thread t3 = (new Thread(new AvroConsumer("random3")));
        t1.start();
        t2.start();
        t3.start();

        try {
            t1.join();
            t2.join();
            t3.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


        System.out.println(AvroConsumer.total);

        System.out.println("Done !!!");
    }

    public int  getCount(){
        return count;
    }

    public  void countMessage() throws Exception {

        byte[] next;

        String zkUrl = "192.168.99.100:2181";
        String groupId = "group1";

        waitTime = "10000";

        configure(zkUrl, groupId);

        start(topic);

        while ((next = getNextMessage()) != null) {
            try {
                DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(_schema);
                Decoder decoder = DecoderFactory.get().binaryDecoder(next, null);
                GenericRecord result = reader.read(null, decoder);

                if (result.get("random").equals(topic.substring(7)))
                    throw new Exception("wrong topic");


                count ++;
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //consumer.shutdown();

    }

    private void configure(String zkUrl, String groupId) {
        kafkaProps.put("zookeeper.connect", zkUrl);
        kafkaProps.put("group.id",groupId);
        kafkaProps.put("auto.commit.interval.ms","1000");
        kafkaProps.put("auto.offset.reset","largest");

        // un-comment this if you want to commit offsets manually
        //kafkaProps.put("auto.commit.enable","false");

        // un-comment this if you don't want to wait for data indefinitely
        kafkaProps.put("consumer.timeout.ms",waitTime);

        config = new ConsumerConfig(kafkaProps);
    }

    private void start(String topic) {
        consumer = Consumer.createJavaConsumerConnector(config);


/* We tell Kafka how many threads will read each topic. We have one topic and one thread */

        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic,new Integer(1));


/* We will use a decoder to get Kafka to convert messages to Strings
        * valid property will be deserializer.encoding with the charset to use.
        * default is UTF8 which works for us */

        DefaultDecoder decoder = new DefaultDecoder(new VerifiableProperties());


/* Kafka will give us a list of streams of messages for each topic.
        In this case, its just one topic with a list of a single stream */

        stream = consumer.createMessageStreams(topicCountMap, decoder, decoder).get(topic).get(0);

    }

    private byte[] getNextMessage() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        try {
            return it.next().message();
        } catch (ConsumerTimeoutException e) {
            System.out.println("waited " + waitTime + " and no messages arrived.");
            return null;
        }
    }


    @Override
    public void run() {

        try {
            countMessage();
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println("--------------"+getCount());
        total += getCount();
    }


}
