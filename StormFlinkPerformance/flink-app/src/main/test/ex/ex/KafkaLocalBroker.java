package ex.ex;

import junit.framework.Assert;
import kafka.admin.AdminUtils;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.producer.Producer;
import kafka.network.SocketServer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData.Record;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.curator.test.TestingServer;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.net.NetUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
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
import java.net.ServerSocket;
import java.util.Properties;


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
        System.out.println("----------kafkaHost--------"+kafkaHost);
        zkPort =   NetUtils.getAvailablePort();
        
        zkConnectionString = "localhost:" + zkPort;
        

        zookeeper = null;
        broker = null;
        try {
            LOG.info("Starting Zookeeper");
            zookeeper = new TestingServer(zkPort, tmpZkDir);

            LOG.info("Starting KafkaServer");
            broker = getKafkaServer(0, tmpKafkaDir);
            
            SocketServer socketServer = broker.socketServer();
            
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

    @AfterClass
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
/*

    @Test
    public void simpleTestTopology() throws Exception {
        String topic = "simpleTestTopic";

        createTestTopic(topic, 1, 1);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(1);

        // add consuming topology:
        DataStreamSource<String> consuming = env.addSource(
                new PersistentKafkaSource<String>(topic, new JavaDefaultStringSchema(), standardCC));
        consuming.addSink(new SinkFunction<String>() {
            private static final long serialVersionUID = 1L;

            int elCnt = 0;
            int start = -1;
            BitSet validator = new BitSet(101);

            @Override
            public void invoke(String value) throws Exception {
                LOG.debug("Got " + value);
                String[] sp = value.split("-");
                int v = Integer.parseInt(sp[1]);
                if (start == -1) {
                    start = v;
                }
                Assert.assertFalse("Received tuple twice", validator.get(v - start));
                validator.set(v - start);
                elCnt++;
                if (elCnt == 100) {
                    // check if everything in the bitset is set to true
                    int nc;
                    if ((nc = validator.nextClearBit(0)) != 100) {
                        throw new RuntimeException("The bitset was not set to 1 on all elements. Next clear:" + nc + " Set: " + validator);
                    }
                    throw new Exception();
                }
            }
        });

        // add producing topology
        DataStream<String> stream = env.addSource(new SourceFunction<String>() {
            private static final long serialVersionUID = 1L;
            boolean running = true;

            @Override
            public void run(SourceFunction.SourceContext<String> ctx) throws Exception {
                LOG.info("Starting source.");
                int cnt = 0;
                while (running) {
                    ctx.collect("kafka-" + cnt++);
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignored) {
                    }
                }
            }

            @Override
            public void cancel() {
                LOG.info("Source got cancel()");
                running = false;
            }
        });
        stream.addSink(new KafkaSink<String>(brokerConnectionString, topic, new JavaDefaultStringSchema()));

        tryExecute(env, "simpletest");
    }
*/

    public static void tryExecute(StreamExecutionEnvironment see, String name) throws Exception {
        try {
            see.execute(name);
        } catch (JobExecutionException good) {
            Throwable t = good.getCause();
            int limit = 0;
            while (!(t instanceof Exception)) {
                if(t == null) {
                    LOG.warn("Test failed with exception", good);
                    Assert.fail("Test failed with: " + good.getMessage());
                }

                t = t.getCause();
                if (limit++ == 20) {
                    LOG.warn("Test failed with exception", good);
                    Assert.fail("Test failed with: " + good.getMessage());
                }
            }
        }
    }

    

    @Test
    public  void testTopology(){
        
        // create topic
        String [] topics = new String [] {"neverwinter2"};//, "random1", "random2", "random3"};
        for (String topic : topics) {
            createTestTopic(topic,1, 1);
        }
        
        // avro producer

        String avroSchemaFileName = "/Users/kidio/message.avsc";// props.getProperty("avro.schema.file");
        String topic = "neverwinter2" ; //props.getProperty("kafka.topic");


        try	{
//          Properties props = new Properties();
//          props.load(new BufferedInputStream(new FileInputStream(configuration)));
            // schema file
            //File avroSchemaFile = new File(avroSchemaFileName);

            Properties props = new Properties();
            props.setProperty("metadata.broker.list", brokerConnectionString);


            ProducerConfig config = new ProducerConfig(props);

            Producer<String, byte[]> producer = new Producer<String, byte[]>(config);

            byte[] avroRecord = jsonToAvro("{\"id\":1,\"random\":1,\"data\":\"duyvk\"}",avroSchemaFileName); //encodeMessage(topic,record,props);
            System.out.println("-----record------"+ avroRecord);

            //Send message to kafka brokers
            KeyedMessage<String, byte[]> data = new KeyedMessage<String, byte[]>(topic, avroRecord);
            //ProducerRecord<String, String> record = new ProducerRecord<String, String>(outputTopic, event.getIp().toString(), event);

            producer.send(data);

//            producer.close();

//            AvroConsumer consumer = new AvroConsumer();
//            System.out.println("-----zkConnectionString2-----"+ zkConnectionString);
//            consumer.countMessage(topic, zkConnectionString, "flink-test");
//            System.out.println("----count----"+ consumer.getCount());

/*

            StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment().setParallelism(4);
            env.getConfig().disableSysoutLogging();
            //env.enableCheckpointing(50);
            env.setNumberOfExecutionRetries(0);

            DataStream<String> kafkaStream = env
                    .addSource(new KafkaSource<String>(brokerConnectionString, topic, new MySimpleStringSchema()));


            kafkaStream.print();
            try {
                env.execute();
            } catch (Exception e) {
                e.printStackTrace();
            }
        */
        
        
        }
        catch (IOException e)
        {
            e.printStackTrace();
        }
        
        
        
        // process
        


        // avro consumer
        
        

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