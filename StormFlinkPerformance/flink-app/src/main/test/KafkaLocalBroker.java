import junit.framework.Assert;
import kafka.admin.AdminUtils;
import kafka.consumer.ConsumerConfig;
import kafka.message.Message;
import kafka.network.SocketServer;
import kafka.producer.KeyedMessage;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import kafka.utils.VerifiableProperties;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.curator.test.TestingServer;
import org.apache.flink.runtime.net.NetUtils;


import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.api.KafkaSource;
import org.apache.zookeeper.server.ServerConfig;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.util.Properties;




import kafka.javaapi.producer.Producer;
import kafka.producer.ProducerConfig;

import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.IndexedRecord;


public class KafkaLocalBroker {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaLocalBroker.class);
    
    private static int zkPort;
    
    private static String kafkaHost;
    private static String  zkConnectionString;
    
    @ClassRule
    public static TemporaryFolder tempFolder = new TemporaryFolder();
    public static File tmpZkDir;
    public static File tmpKafkaDir ;
    
    private static TestingServer zookeeper;
    private static KafkaServer broker;
    private static String  brokerConnectionString = "";
    
    private static ConsumerConfig standardCC;
    
    private static ZkClient zkClient;
    
    @BeforeClass
    public static void prepare() throws  IOException {
        LOG.info("Starting KafkaBrokerLocal.prepare()");
        
        tmpZkDir = tempFolder.newFolder();
        tmpKafkaDir = tempFolder.newFolder();

        kafkaHost = "localhost";//InetAddress.getLocalHost().getHostName();
        System.out.println("-------------------"+kafkaHost);
        zkPort = NetUtils.getAvailablePort();
        System.out.println("-------------------"+zkPort);
//        zkPort = findRandomAvaiPort();
        
        zkConnectionString = "localhost:" + zkPort;
        
        
        zookeeper = null;
        broker = null;
        try {
            LOG.info("Starting Zookeeper");
            zookeeper = new TestingServer(zkPort, tmpZkDir);

            LOG.info("Starting KafkaServer");
            broker = getKafkaServer(0, tmpKafkaDir);
            
            SocketServer socketServer = broker.socketServer();
//            brokerConnectionString = socketServer.host() + ":"+ socketServer.port();
            brokerConnectionString =   "localhost:"+ socketServer.port();

            LOG.info("Zookeeper and KafkaServer started");
            
        } catch (Throwable t) {
            LOG.warn("Test failed with exceptions", t);
            Assert.fail("Test failed with: " + t.getMessage());
        }

        Properties cProps = new Properties();
        cProps.setProperty("zookeeper.connect", zkConnectionString);
        cProps.setProperty("group.id", "flink-test");
        cProps.setProperty("auto.commit.enable", "false");
        cProps.setProperty("auto.offset.reset", "smallest");

        standardCC = new ConsumerConfig(cProps);
        zkClient = new ZkClient(standardCC.zkConnect(), standardCC.zkSessionTimeoutMs(), standardCC.zkConnectionTimeoutMs(), new MyKafkaZKStringSerializer());
    }

    //@AfterClass
    public static void shutDownServices() {
        LOG.info("Shutdown all services");
        broker.shutdown();
        if (zookeeper != null) {
            try {
                zookeeper.stop();
            } catch (IOException e) {
                LOG.warn("Zk.stop() failed",e);
            }
        }
        zkClient.close();
    }

    
    @Test
    public  void testTopology(){
        
        // create topic
        String [] topics = new String [] {"neverwinter2", "random1", "random2", "random3"};
        for (String topic : topics) {
            createTestTopic(topic,1, 1);
        }
        
        // avro producer

        String avroSchemaFileName = "/tmp/message.avsc";// props.getProperty("avro.schema.file");
        String topic = "neverwinter" ; //props.getProperty("kafka.topic");


        try	{
//          Properties props = new Properties();
//          props.load(new BufferedInputStream(new FileInputStream(configuration)));

            // schema file
            //File avroSchemaFile = new File(avroSchemaFileName);

            Properties props = new Properties();
            props.setProperty("metadata.broker.list", brokerConnectionString);
            
            System.out.println("------brokerConnectionString------"+brokerConnectionString);

            ProducerConfig config = new ProducerConfig(props);

            Producer<String, byte[]> producer = new Producer<String, byte[]>(config);

            
            //Record record = fillRecord(fillAvroTestSchema(avroSchemaFile));
            byte[] avroRecord = jsonToAvro("{\"id\":1,\"random\":1,\"data\":\"duyvk\"}",avroSchemaFileName); //encodeMessage(topic,record,props);

            System.out.println("--------------"+avroRecord +"--------------");

            // Print ID received from avro schema repo server
            /*for (int n = 1;n <= 4 ; n++){
                System.out.print(avroRecord[n]);
            }
            System.out.println();
*/
            //Send message to kafka brokers
            KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(topic, avroRecord);
            producer.send(data);

            producer.close();
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        
        // process
       /* StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment().setParallelism(4);

        DataStream<String> kafkaStream = env
                .addSource(new KafkaSource<String>(brokerConnectionString, topic, new MySimpleStringSchema()));


        kafkaStream.print();
        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
*/

        // avro consumer
        
        AvroConsumer consumer = new AvroConsumer();
        System.out.println("-----zkConnectionString2-----"+ zkConnectionString);
        consumer.countMessage(topic, zkConnectionString);;

    }
    
    private void createTestTopic (String topic, int numOfPartitions, int replicationFactor){
        LOG.info("Creating topic: "+ topic);
        Properties topicConfig = new Properties();
        AdminUtils.createTopic(zkClient, topic, numOfPartitions, replicationFactor, topicConfig);
    }

    
    
    public static class MyKafkaZKStringSerializer implements ZkSerializer {

        @Override
        public byte[] serialize(Object data) throws ZkMarshallingError {
            try {
                return ((String) data).getBytes("UTF-8");
            } catch (UnsupportedEncodingException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Object deserialize(byte[] bytes) throws ZkMarshallingError {
            if (bytes == null) {
                return null;
            } else {
                try {
                    return new String(bytes, "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }

    private static KafkaServer getKafkaServer(int brokerId, File tempFolder) {
        Properties kafkaProperties = new Properties();
        
        int kafkaPort = NetUtils.getAvailablePort();
        
        kafkaProperties.put("advertised.host.name", kafkaHost);
        kafkaProperties.put("port", Integer.toString(kafkaPort));
        kafkaProperties.put("broker.id", Integer.toString(brokerId));
        kafkaProperties.put("log.dir", tempFolder.toString());
        kafkaProperties.put("zookeeper.connect", zkConnectionString);
        //kafkaProperties.put("");
        KafkaConfig kafkaConfig = new KafkaConfig(kafkaProperties);

        KafkaServer server = new KafkaServer(kafkaConfig, new KafkaLocalSystemTime());
        server.startup();
        return server;
    }
    
    private static Integer findRandomAvaiPort() throws IOException{
        try(
            ServerSocket ss = new ServerSocket(0);
        ){
            return ss.getLocalPort();
        }
    }

    @SuppressWarnings("deprecation")
    private static Schema fillAvroTestSchema(File jsonSchemaFile) throws IOException{
        //Schema.Parser schemaParser = Schema.Parser();
        return Schema.parse(jsonSchemaFile);
    }

    private static Record fillRecord(Schema schema){
        Record record = new Record(schema);
        //record.put("test","Hello World");
        return record;
    }

    /*private static byte[] encodeMessage(String topic, IndexedRecord record, Properties props){
        KafkaAvroMessageEncoder encoder = new KafkaAvroMessageEncoder(topic, null);
        encoder.init(props, topic);
        return encoder.toBytes(record);
    }*/

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

class KafkaLocalSystemTime implements Time {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaLocalSystemTime.class);

    @Override
    public long milliseconds() {
        return System.currentTimeMillis();
    }

    public long nanoseconds() {
        return System.nanoTime();
    }

    @Override
    public void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            LOG.warn("Interruption", e);
        }
    }

}

class ZooKeeperLocal {

    ZooKeeperServerMain zooKeeperServer;

    public ZooKeeperLocal(Properties zkProperties) throws FileNotFoundException, IOException {
        QuorumPeerConfig quorumConfiguration = new QuorumPeerConfig();
        try {
            quorumConfiguration.parseProperties(zkProperties);
        } catch(Exception e) {
            throw new RuntimeException(e);
        }

        zooKeeperServer = new ZooKeeperServerMain();
        final ServerConfig configuration = new ServerConfig();
        configuration.readFrom(quorumConfiguration);


        new Thread() {
            public void run() {
                try {
                    zooKeeperServer.runFromConfig(configuration);
                } catch (IOException e) {
                    System.out.println("ZooKeeper Failed");
                    e.printStackTrace(System.err);
                }
            }
        }.start();
    }
}