
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

public class KafkaConsumer implements Runnable{

    public static Logger LOG = LoggerFactory.getLogger(KafkaConsumer.class);

    private Properties kafkaProps;
    private ConsumerConnector consumer;
    private ConsumerConfig config;
    private KafkaStream<byte[], byte[]> stream;
    private Schema schema;

    private String zkUrl;
    private String topic;
    private int timeout;

    private int count;
    public static int totalMessages = 0;

    public KafkaConsumer(String zkUrl, String topic, int timeout){

        this.topic = topic;
        this.zkUrl = zkUrl;
        this.timeout = timeout;

        kafkaProps = new Properties();

        count = 0;
        try {
            // schema of avro message is fixed in this context
            schema = new Schema.Parser().parse(new File("src/main/resources/message.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main (String [] args){

        String zkUrl = "localhost:2181";
        int timeout =0,  numOfExpectedMsg = 0;
        String[] topics  = new String[] {"random1", "random2", "random3"};


        // reading input parameters
        if (args.length == 4) {
            zkUrl = args[0];
            topics = args[1].split(",");
            timeout = Integer.parseInt(args[2].trim());
            numOfExpectedMsg = Integer.parseInt(args[3].trim());
            LOG.debug("Starting : KafkaConsumer " + zkUrl + " "+ topics + " " + timeout);
        } else {
            System.err.println("Usage: KafkaConsumer <host>:<port>  <topics> <timeout> <number of messages>");
            System.err.println("\t <host>       :  zookeeper host");
            System.err.println("\t <port>       :  zookeeper port");
            System.err.println("\t <topics>     : list of kafka topics");
            System.err.println("\t <timeout>    : consumer timeout in milliseconds  ");
            System.err.println("\t <number of messages> : expect this number of messages to arrive ");
            System.exit(0);
        }


        // init a thread for each topic
        Thread [] consumers = new Thread[topics.length];

        for (int i = 0 ; i < consumers.length; i ++) {
            consumers[i] = (new Thread(new KafkaConsumer(zkUrl, topics[i], timeout)));
            consumers[i].start();
            LOG.debug("Starting listening consumer for topic " + topics[i]);
        }


        // waiting for all threads to finish
        try {
            for (int i = 0 ; i < consumers.length; i ++) {
                consumers[i].join();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // conclude
        int numOfReceivedMsg = KafkaConsumer.totalMessages;
        LOG.info("Total received messages in all topics: " + numOfReceivedMsg);

        if (numOfReceivedMsg == numOfExpectedMsg)
            System.out.println("All "+ numOfExpectedMsg + " has received !");
        else {
            int missingMsg = (numOfExpectedMsg - numOfReceivedMsg);
            if (missingMsg > 0 )
                System.err.println(missingMsg +" messages are missing");
            else
                System.err.println("Perhaps some messages have not been consumed before starting the verification");
        }
        LOG.debug("Finish Verification process!");
    }

    @Override
    public void run() {

        try {
            countMessage();
        } catch (Exception e) {
            LOG.error("One of messages fell into a wrong topic of [+"+topic+"+]");
        }

        LOG.info("Topic [" + topic + "] has received " + getCount() + " correct messages.");
        totalMessages += getCount();

    }

    /**
     * * Count number of received messages in the topic
     * * 
     * @throws Exception : when receiving a message of another topic
     */
    public  void countMessage() throws  Exception{

        byte[] next;
        String groupId = "group1";

        // configure consumer-zookeeper connection
        configure(zkUrl, groupId);

        // start listening to kafka topic
        start(topic);

        //  count received message and check "random" field
        while ((next = getNextMessage()) != null) {
            try {
                DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
                Decoder decoder = DecoderFactory.get().binaryDecoder(next, null);
                GenericRecord result = reader.read(null, decoder);

                if (result.get("random").equals(topic.substring(7))) {
                    throw new Exception("wrong topic");
                }

                count ++;

            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        consumer.shutdown();
    }


    public int  getCount(){
        return count;
    }


    private void configure(String zkUrl, String groupId) {

        kafkaProps.put("zookeeper.connect", zkUrl);
        kafkaProps.put("group.id",groupId);
        kafkaProps.put("auto.commit.interval.ms","1000");
        kafkaProps.put("auto.offset.reset","largest");
        kafkaProps.put("consumer.timeout.ms", timeout +"");

        config = new ConsumerConfig(kafkaProps);
    }


    private void start(String topic) {
        consumer = Consumer.createJavaConsumerConnector(config);

        //We have one topic and one thread
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic,new Integer(1));

        DefaultDecoder decoder = new DefaultDecoder(new VerifiableProperties());

        //one topic with a list of a single stream 
        stream = consumer.createMessageStreams(topicCountMap, decoder, decoder).get(topic).get(0);
    }


    private byte[] getNextMessage() {
        ConsumerIterator<byte[], byte[]> it = stream.iterator();

        try {
            return it.next().message();
        } catch (ConsumerTimeoutException e) {
            LOG.info("waited " + timeout + " and no messages arrived.");
            return null;
        }
    }
}
