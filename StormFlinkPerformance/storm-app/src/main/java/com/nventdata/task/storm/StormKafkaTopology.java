package com.nventdata.task.storm;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.jboss.netty.handler.codec.embedder.DecoderEmbedder;
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
import backtype.storm.Constants;
import backtype.storm.LocalCluster;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.StormSubmitter;
import backtype.storm.tuple__init;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import kafka.message.Message;


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
		builder.setSpout("avro", new KafkaSpout(kafkaConfig));//, topologyProperties.getKafkaSpoutParallelism());
		
		builder.setBolt("transform", new KeyExtractor()).shuffleGrouping("avro");
		
		builder.setBolt("print", new PrinterBolt()).shuffleGrouping("transform");
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
        	
        	collector.emit(new Values("word",tuple.getStringByField("name")));
        }
    }
	
	public static class KeyExtractor extends BaseRichBolt{
		OutputCollector _collector;
		Schema _schema;
		

		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("name"));
		}

		@Override
		public void prepare(Map stormConf, TopologyContext context,
				OutputCollector collector) {
			_collector = collector;
			try {
				_schema = new Schema.Parser().parse(new File ("src/main/resources/message.avsc"));
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		@Override
		public void execute(Tuple input) {
			// TODO convert avro to json
			Message message = new Message((byte[])((TupleImpl) input).get("bytes"));
			System.out.println(message.toString()+ "------------");
			
            ByteBuffer bb = message.payload();

            if (bb == null)
            	System.exit(1);
            byte[] avroMessage = new byte[bb.remaining()];
            bb.get(avroMessage, 0, avroMessage.length);
            
			try {
				DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(_schema);
				Decoder decoder = DecoderFactory.get().binaryDecoder(avroMessage, null);
	            GenericRecord result = reader.read(null, decoder);
	            System.out.println(result.get("random")+"--------------------");
				_collector.emit(new Values(result.get("random")));
			} catch (IOException e) {
				e.printStackTrace();
			}

            _collector.ack(input);
			// TODO get data at "random"
			
			// emit
			
		}
		
		
	}
	
	public static void main(String[] args) throws Exception {
		String propertiesFile = args[0];
		TopologyProperties topologyProperties = new TopologyProperties(propertiesFile);
		StormKafkaTopology topology = new StormKafkaTopology(topologyProperties);
		topology.runTopology();
	}
}
