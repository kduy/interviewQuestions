package ex.ex.forunitest;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by kidio on 31/08/15.
 */
public class Main {
    public static void main(String [] args) throws Exception {
        EmbeddedZookeeper embeddedZookeeper = new EmbeddedZookeeper(2181);
        List<Integer> kafkaPorts = new ArrayList<Integer>();
        // -1 for any available port
        kafkaPorts.add(-1);
        //kafkaPorts.add(-1);
        EmbeddedKafkaCluster embeddedKafkaCluster = new EmbeddedKafkaCluster(embeddedZookeeper.getConnection(), new Properties(), kafkaPorts);
        embeddedZookeeper.startup();
        System.out.println("### Embedded Zookeeper connection: " + embeddedZookeeper.getConnection());
        embeddedKafkaCluster.startup();
        System.out.println("### Embedded Kafka cluster broker list: " + embeddedKafkaCluster.getBrokerList());
        Thread.sleep(10000);
        embeddedKafkaCluster.shutdown();
        embeddedZookeeper.shutdown();

    }
}
