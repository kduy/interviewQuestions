package com.nventdata.task.storm.topology;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import com.nventdata.task.storm.bolt.SplitStreamBolt;
import com.nventdata.task.storm.utils.TopologyProperties;


/**
 * @author pablo
 * 
 * The main class to split an avro message stream from kafka source, 
 * split based on the `random` field then feed the split stream to kafka sink
 *
 */
public class StormKafkaTopology {
	public static final Logger LOG = LoggerFactory
			.getLogger(StormKafkaTopology.class);

	private final TopologyProperties topologyProperties;
    static final String AVRO_MSG_SCHEMA_FILE_PATH = "src/main/resources/message.avsc";


    public StormKafkaTopology(TopologyProperties topologyProperties) {
		this.topologyProperties = topologyProperties;
	}
	
	/**
	 * submit the built topology with local/cluster mode
	 * 
	 * @throws Exception
	 */
	public void runTopology() throws Exception{
		
		StormTopology stormTopology = buildTopology();
		String stormExecutionMode = topologyProperties.getStormExecutionMode();

		switch (stormExecutionMode){
			case ("cluster"):
				StormSubmitter.submitTopology(topologyProperties.getTopologyName(), topologyProperties.getStormConfig(), stormTopology);
				break;
			case ("local"):
			default:
				LocalCluster cluster = new LocalCluster();
				
				Properties props = new Properties();
	            props.put("metadata.broker.list", topologyProperties.getKafkaBrokerList());
	            props.put("request.required.acks", "1");
	            props.put("serializer.class", "kafka.serializer.DefaultEncoder");
				topologyProperties.getStormConfig().put(KafkaBolt.KAFKA_BROKER_PROPERTIES,props);
	            
				cluster.submitTopology(topologyProperties.getTopologyName(), topologyProperties.getStormConfig(), stormTopology);
		}	
	}

	/**
	 * 	build the topology
	 * 
	 * @return
	 */
	private StormTopology buildTopology(){
		// zookeeper 
		BrokerHosts kafkaBrokerHosts = new ZkHosts(topologyProperties.getZookeeperHosts());
		
		// kafka
		String kafkaTopic = topologyProperties.getKafkaTopic();
		
		SpoutConfig kafkaConfig = new SpoutConfig(kafkaBrokerHosts, kafkaTopic, "", kafkaTopic);
		kafkaConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		kafkaConfig.forceFromStart = topologyProperties.isKafkaStartFromBeginning();
		kafkaConfig.scheme = new SchemeAsMultiScheme(new AvroScheme(topologyProperties, readAvroMessageSchema()));
		
		// build a Storm topology
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("avro", new KafkaSpout(kafkaConfig));//, topologyProperties.getKafkaSpoutParallelism());
		builder.setBolt("split", new SplitStreamBolt(readAvroMessageSchema())).shuffleGrouping("avro");
        
		builder.setBolt("forwardToKafka1", createKafkaBoltWithTopic("random1")).shuffleGrouping("split", "random1");
        builder.setBolt("forwardToKafka2", createKafkaBoltWithTopic("random2")).shuffleGrouping("split", "random2");
        builder.setBolt("forwardToKafka3", createKafkaBoltWithTopic("random3")).shuffleGrouping("split", "random3");
        
		return builder.createTopology();
	}
    
    private String readAvroMessageSchema() {

        try {
            String filePath = (String)topologyProperties.getStormConfig().getOrDefault("avro.schema.filePath",AVRO_MSG_SCHEMA_FILE_PATH);
            BufferedReader br = new BufferedReader(new FileReader(filePath));
            String line;
            StringBuilder sb = new StringBuilder();
            while ((line = br.readLine()) != null) {
                sb.append(line);
            }
            return  sb.toString();
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

	/**
	 * create a bold to forward message to Kafka topic
	 * 
	 * @param kafkaTopic
	 * @return
	 */
	private KafkaBolt<String, byte[]> createKafkaBoltWithTopic(String kafkaTopic) {
		return new KafkaBolt<String, byte[]> ()
        		.withTopicSelector( new DefaultTopicSelector(kafkaTopic))
  				.withTupleToKafkaMapper( new FieldNameBasedTupleToKafkaMapper<String, byte[]>());
	}

    
	public static void main(String[] args) throws Exception {
        String propertiesFile;
        if (args.length == 1) {
            propertiesFile = args[0];
        } else {
            System.err.println("Usage: StormKafkaTopology <Topology Property File>");
            return;
        }

		TopologyProperties topologyProperties = new TopologyProperties(propertiesFile);
		StormKafkaTopology topology = new StormKafkaTopology(topologyProperties);
		topology.runTopology();
	}
}
