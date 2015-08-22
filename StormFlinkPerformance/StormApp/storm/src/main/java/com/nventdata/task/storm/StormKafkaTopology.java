package com.nventdata.task.storm;

import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import storm.kafka.bolt.mapper.FieldNameBasedTupleToKafkaMapper;
import storm.kafka.bolt.selector.DefaultTopicSelector;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


public class StormKafkaTopology {
	public static final Logger LOG = LoggerFactory
			.getLogger(StormKafkaTopology.class);

	private final TopologyProperties topologyProperties;
	
	public StormKafkaTopology(TopologyProperties topologyProperties) {
		this.topologyProperties = topologyProperties;
	}
	
	public void runTopology() throws Exception{

		StormTopology stormTopology = buildTopology();
		String stormExecutionMode = topologyProperties.getStormExecutionMode();
	
		Config config = new Config();
        config.put(Config.TOPOLOGY_TRIDENT_BATCH_EMIT_INTERVAL_MILLIS, 2000);

		switch (stormExecutionMode){
			case ("cluster"):
				StormSubmitter.submitTopology(topologyProperties.getTopologyName(), topologyProperties.getStormConfig(), stormTopology);
				break;
			case ("local"):
			default:
				LocalCluster cluster = new LocalCluster();
				Properties props = new Properties();
	            props.put("metadata.broker.list", "localhost:9092");
	            props.put("request.required.acks", "1");
	            props.put("serializer.class", "kafka.serializer.StringEncoder");
				topologyProperties.getStormConfig().put(KafkaBolt.KAFKA_BROKER_PROPERTIES,props);
	            config.put(KafkaBolt.KAFKA_BROKER_PROPERTIES, props);
	            cluster.submitTopology(topologyProperties.getTopologyName(), topologyProperties.getStormConfig(), stormTopology);
	            /*Thread.sleep(topologyProperties.getLocalTimeExecution());
				cluster.killTopology(topologyProperties.getTopologyName());
				cluster.shutdown();
				System.exit(0);*/
		}	
	}
	
	private StormTopology buildTopology()
	{
		/*
		BrokerHosts kafkaBrokerHosts = new ZkHosts(topologyProperties.getZookeeperHosts());
		String kafkaTopic = topologyProperties.getKafkaTopic();
		SpoutConfig kafkaConfig = new SpoutConfig(kafkaBrokerHosts, kafkaTopic, "/storm/kafka/"+topologyProperties.getTopologyName(), kafkaTopic);
		kafkaConfig.forceFromStart = topologyProperties.isKafkaStartFromBeginning();
		*/

		BrokerHosts kafkaBrokerHosts = new ZkHosts(topologyProperties.getZookeeperHosts());
		String kafkaTopic = topologyProperties.getKafkaTopic();
		SpoutConfig kafkaConfig = new SpoutConfig(kafkaBrokerHosts, kafkaTopic, "", kafkaTopic);
		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		TopologyBuilder builder = new TopologyBuilder();

		/*
		builder.setSpout("KafkaSpout", new KafkaSpout(kafkaConfig), topologyProperties.getKafkaSpoutParallelism());
		builder.setBolt("FilterBolt", new FilterMessageBolt(), topologyProperties.getFilterBoltParallelism()).shuffleGrouping("KafkaSpout");
		builder.setBolt("TCPBolt", new TCPBolt(), topologyProperties.getTcpBoltParallelism()).shuffleGrouping("FilterBolt");
		*/
		builder.setSpout("words", new KafkaSpout(kafkaConfig), topologyProperties.getKafkaSpoutParallelism());
		builder.setBolt("print", new PrinterBolt()).shuffleGrouping("words");
		KafkaBolt <String, String> bolt = new KafkaBolt<String, String> ()
        		.withTopicSelector( new DefaultTopicSelector("storm-word"))
  				.withTupleToKafkaMapper( new FieldNameBasedTupleToKafkaMapper<String, String>());
        builder.setBolt("forwardToKafka", bolt).shuffleGrouping("print");
        
		return builder.createTopology();
	}
	
	public static class PrinterBolt extends BaseBasicBolt {
        @Override
        public void declareOutputFields(OutputFieldsDeclarer declarer) {
        	declarer.declare(new Fields("key", "message"));
        }

        @Override
        public void execute(Tuple tuple, BasicOutputCollector collector) {
        	collector.emit(new Values("word",tuple.toString()));
        }
    }
	
	public static void main(String[] args) throws Exception {
		String propertiesFile = args[0];
		TopologyProperties topologyProperties = new TopologyProperties(propertiesFile);
		StormKafkaTopology topology = new StormKafkaTopology(topologyProperties);
		topology.runTopology();
	}
}
