package com.nventdata.task.storm;

import java.io.*;
import java.util.List;
import java.util.Properties;

import com.nventdata.task.storm.performance.Performance;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;

import backtype.storm.spout.Scheme;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class AvroScheme implements Scheme {
	
	public static final String AVRO_SCHEME_KEY = "avro";
    private String avroMessageSchema ;

    static  final Performance perf = new Performance("storm", 100, 1000, "/tmp/metrics");


    public AvroScheme ( String schema) {

        avroMessageSchema = schema ;

    }

	@Override
	public List<Object> deserialize(byte[] ser) {
		return new Values(deserializeAvro(ser));
	}
	
	public  String deserializeAvro(byte[] avroMsg) {
		try {
			Schema schema = new Schema.Parser().parse(avroMessageSchema);
			DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
			Decoder decoder = DecoderFactory.get().binaryDecoder(avroMsg, null);
            GenericRecord result = reader.read(null, decoder);

            perf.track(1, avroMsg.length);
            
            return result.toString();
		} catch (IOException e) {
			e.printStackTrace();
			throw new RuntimeException(e);
		}
    }

	@Override
	public Fields getOutputFields() {
		return new Fields(AVRO_SCHEME_KEY);
	}

}
