import io.netty.util.NetUtil;
import junit.framework.Assert;
import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.consumer.ConsumerConfig;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.javaapi.producer.Producer;
import kafka.message.MessageAndOffset;
import kafka.network.SocketServer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import kafka.server.KafkaConfig;
import kafka.server.KafkaServer;
import kafka.utils.Time;
import kafka.utils.Utils;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.apache.curator.test.TestingServer;
import org.apache.flink.runtime.net.NetUtils;
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
import scala.tools.cmd.gen.AnyVals;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
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

        kafkaHost = InetAddress.getLocalHost().getHostName();
        zkPort = NetUtils.getAvailablePort();
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
            brokerConnectionString = socketServer.host() + ":"+ socketServer.port();
            
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

    @Test
    public  void simpleTest() {
        Assert.assertTrue(true);
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