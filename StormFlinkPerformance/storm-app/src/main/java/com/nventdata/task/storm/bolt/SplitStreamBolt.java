package com.nventdata.task.storm.bolt;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.json.JSONObject;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;


/**
 * @author kidio
 * 
 * class SplitStreamBolt to split the input stream into 3 output streams
 *	based on `random` field
 *
 */
public class SplitStreamBolt extends BaseBasicBolt {

    private String avroMessageSchema ;
    
    public SplitStreamBolt(String schema) {
        avroMessageSchema = schema;
    }
    
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
    	declarer.declareStream("random1", new Fields("random", "message"));
    	declarer.declareStream("random2", new Fields("random", "message"));
    	declarer.declareStream("random3", new Fields("random", "message"));
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
    	JSONObject jsonObject = new JSONObject(tuple.getStringByField("avro"));
        String randomField = "random"+ jsonObject.getInt("random");

        String json = tuple.getStringByField("avro");
    	collector.emit (randomField, new Values(randomField,jsonToAvro(json)));
    }

	private byte[] jsonToAvro(String json) {
		InputStream input = null;
        GenericDatumWriter<GenericRecord> writer = null;
        Encoder encoder = null;
        ByteArrayOutputStream output = null;
        try {
            Schema schema = new Schema.Parser().parse(avroMessageSchema);
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
        } catch (IOException e) {
			e.printStackTrace();
		} finally {
            try { input.close(); } catch (Exception e) { }
        }
		return json.getBytes();
	}
}
