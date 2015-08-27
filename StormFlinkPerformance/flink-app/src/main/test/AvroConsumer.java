import kafka.consumer.*;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
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
import kafka.serializer.StringDecoder;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AvroConsumer {

    public static Logger LOG = LoggerFactory.getLogger(AvroConsumer.class);
    
    private Properties kafkaProps = new Properties();
    private ConsumerConnector consumer;
    private ConsumerConfig config;
    private KafkaStream<String, byte[]> stream;
    private String waitTime;
    private int count;
    
    public AvroConsumer(){
        count = 0;
    }

    public int getCount(){
        return count;
    }

    
    public  void countMessage(String topic, String zkUrl) {

        byte[] next;

        String groupId = "flink-test";

        waitTime = "1000";

        configure(zkUrl, groupId);


        start(topic);
        
        
        
        if (getNextMessage() == null)
            System.out.println("ffffffffffffffffffffffff");

        while ((next = getNextMessage()) != null) {
            try {
                Schema _schema = new Schema.Parser().parse(new File("/tmp/message.avsc"));
                DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(_schema);
                Decoder decoder = DecoderFactory.get().binaryDecoder(next, null);
                GenericRecord result = reader.read(null, decoder);
                System.out.println("------result-----"+result);
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
        /*kafkaProps.put("auto.commit.interval.ms","1000");
        kafkaProps.put("auto.offset.reset","largest");
*/
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
        DefaultDecoder decoder = new DefaultDecoder(new VerifiableProperties(kafkaProps));

        StringDecoder strDecoder = new StringDecoder(new VerifiableProperties());
        /* Kafka will give us a list of streams of messages for each topic.
        In this case, its just one topic with a list of a single stream */
        stream = consumer.createMessageStreams(topicCountMap, strDecoder, decoder).get(topic).get(0);

    }

    private Properties createConsumerConfig(String zookeeper, String groupId, String url) {
        Properties props = new Properties();
        props.put("zookeeper.connect", zookeeper);
        props.put("group.id", groupId);
        props.put("auto.commit.enable", "false");
        props.put("auto.offset.reset", "smallest");
        props.put("schema.registry.url", url);
        props.put("specific.avro.reader", true);

        return props;
    }
    
    private byte[] getNextMessage() {
        ConsumerIterator<String, byte[]> it = stream.iterator();

        //MessageAndMetadata messageAndMetadata = it.next();
        try {
            //System.out.println("--------key-----:"+ ()messageAndMetadata.key());
            return it.next().message();
        } catch (ConsumerTimeoutException e) {
            System.out.println("waited " + waitTime + " and no messages arrived.");
            return null;
        }
    }
}