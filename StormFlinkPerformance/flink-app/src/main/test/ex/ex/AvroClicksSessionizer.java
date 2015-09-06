package ex.ex;

//import JavaSessionize.avro.LogLine;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class AvroClicksSessionizer {
    private final ConsumerConnector consumer;
    //private final KafkaProducer<String, LogLine> producer;
    private final String inputTopic;
    private final String outputTopic;
    private String zookeeper;
    private String groupId;
    private String url;
    private Map<String, SessionState> state = new HashMap<String, SessionState>();


    public static void main(String[] args) {


        // currently hardcoding a lot of parameters, for simplicity
        String zookeeper = "localhost:2181";
        String groupId = "AvroClicksSessionizer";
        String inputTopic = "random1";
        String outputTopic = "sessionized_clicks";
        String url = "localhost";

        AvroClicksSessionizer sessionizer = new AvroClicksSessionizer(zookeeper, groupId, inputTopic, outputTopic, url);
        sessionizer.run();

    }

    public AvroClicksSessionizer(String zookeeper, String groupId, String inputTopic, String outputTopic, String url) {
        this.consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                new ConsumerConfig(createConsumerConfig(zookeeper, groupId, url)));
        //this.producer = getProducer(outputTopic, url);

        this.zookeeper = zookeeper;
        this.groupId = groupId;
        this.inputTopic = inputTopic;
        this.outputTopic = outputTopic;
        this.url = url;
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

    private void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        // Hard coding single threaded consumer
        topicCountMap.put(inputTopic, new Integer(1));

        Properties props = createConsumerConfig(zookeeper, groupId, url);
        VerifiableProperties vProps = new VerifiableProperties(props);

        // Create decoders for key and value
        KafkaAvroDecoder avroDecoder = new KafkaAvroDecoder(vProps);
        StringDecoder stringDecoder = new StringDecoder(new VerifiableProperties());

        KafkaStream stream = consumer.createMessageStreams(topicCountMap, stringDecoder, avroDecoder).get(inputTopic).get(0);
        ConsumerIterator it = stream.iterator();

        System.out.println("Ready to start iterating wih properties: " + props.toString());
        System.out.println("Reading topic:" + inputTopic);

        while (it.hasNext()) {
            MessageAndMetadata messageAndMetadata = it.next();

            //String ip = (String) messageAndMetadata.key();
            System.out.println(messageAndMetadata.message());
            /*MessageAndMetadata messageAndMetadata = it.next();

            String ip = (String) messageAndMetadata.key();

            // Once we release a new version of the avro deserializer that can return SpecificData, the deep copy will be unnecessary
            GenericRecord genericEvent = (GenericRecord) messageAndMetadata.message();
            LogLine event = (LogLine) SpecificData.get().deepCopy(LogLine.SCHEMA$, genericEvent);

            SessionState oldState = state.get(ip);
            int sessionId = 0;
            if (oldState == null) {
                state.put(ip, new SessionState(event.getTimestamp(), 0));
            } else {
                sessionId = oldState.getSessionId();
                // if the old timestamp is more than 30 minutes older than new one, we have a new session
                if (oldState.getLastConnection() < event.getTimestamp() - (30 * 60 * 1000))
                    sessionId = sessionId + 1;
                SessionState newState = new SessionState(event.getTimestamp(), sessionId);
                state.put(ip, newState);
            }
            event.setSessionid(sessionId);
            System.out.println(event.toString());
            ProducerRecord<String, LogLine> record = new ProducerRecord<String, LogLine>(outputTopic, event.getIp().toString(), event);
            producer.send(record);*/
        }

    }

    /*private KafkaProducer<String, LogLine> getProducer(String topic, String url) {
        Properties props = new Properties();
        // hardcoding the Kafka server URI for this example
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("key.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer");
        props.put("schema.registry.url", url);

        KafkaProducer<String, LogLine> producer = new KafkaProducer<String, LogLine>(props);
        return producer;
    }*/


}
class SessionState {
    private long lastConnection;
    private int sessionId;

    public SessionState(long lastConnection, int sessionId) {
        this.lastConnection = lastConnection;
        this.sessionId = sessionId;
    }

    public int getSessionId() {
        return sessionId;
    }

    public void setSessionId(int sessionId) {
        this.sessionId = sessionId;
    }

    public long getLastConnection() {
        return lastConnection;
    }

    public void setLastConnection(long lastConnection) {
        this.lastConnection = lastConnection;
    }
}