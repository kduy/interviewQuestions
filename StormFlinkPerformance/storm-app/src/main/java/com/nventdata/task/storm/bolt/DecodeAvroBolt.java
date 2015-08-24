package com.nventdata.task.storm.bolt;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;

import kafka.message.Message;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.commons.codec.binary.Hex;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.TupleImpl;
import backtype.storm.tuple.Values;


/**
 * @author kidio
 *
 * class DecodeAvroBolt to decode incoming avro messages
 *
 */
public  class DecodeAvroBolt extends BaseRichBolt{
	OutputCollector _collector;
	Schema _schema;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("random", "message"));
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
		Message message = new Message((byte[]) ((TupleImpl) input).get("str").toString().getBytes());
					
        ByteBuffer bb = message.payload();
        byte[] avroMessage = new byte[23];
        bb.position(bb.capacity()-23);
        bb.get(avroMessage, 0, avroMessage.length);
		
		try {
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(_schema);
			Decoder decoder = DecoderFactory.get().binaryDecoder(avroMessage, null);
            GenericRecord result = reader.read(null, decoder);
            System.out.println(result.toString());
            _collector.emit(new Values("random"+result.get("random"), Hex.encodeHexString(avroMessage)));
		} catch (IOException e) {
			e.printStackTrace();
		}
        _collector.ack(input);
	}
}
