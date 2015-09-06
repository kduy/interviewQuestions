package com.nventdata.task.flink.topology;


import com.nventdata.task.flink.performance.Performance;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import java.io.*;
import java.util.Properties;

/**
 * * Serialize/Deserialize messages btw json and avro format
 */
public  class AvroSerializationSchema implements SerializationSchema<String, byte[]>, DeserializationSchema<String> {
    
    final Performance perf;
    public static final String AVRO_MSG_SCHEMA_FILE_PATH = "src/main/resources/message.avsc";

    private String schemaStr;
    /*static final String schemaStr = "{" +
            " \"name\": \"kafkatest\"," +
            " \"namespace\": \"test\"," +
            " \"type\": \"record\"," +
            " \"fields\": [" +
            " {\"name\": \"id\", \"type\": \"int\"}," +
            " {\"name\": \"random\", \"type\": \"int\"}," +
            " {\"name\": \"data\", \"type\": \"string\"}" +
            " ]" +
            " }";
    */
    public AvroSerializationSchema(Properties properties, boolean measurePerformance ) {

        if (measurePerformance) {
            perf = new Performance(
                    properties.getProperty("performance.name", "app"),
                    Integer.parseInt(properties.getProperty("performance.interval", "0")),
                    Integer.parseInt(properties.getProperty("performance.dump.interval", "0")),
                    properties.getProperty("performance.dir.output", "./")
            );
        } else
            perf = null;
        
        String schemaFilePath = properties.getProperty("avro.schema.filePath", AVRO_MSG_SCHEMA_FILE_PATH);
        try {
            BufferedReader br = new BufferedReader(new FileReader(schemaFilePath));
            String line ;
            StringBuilder sb = new StringBuilder();
            while ((line = br.readLine())!=null){
                sb.append(line);
            }
            schemaStr = sb.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    @Override
    public String deserialize(byte[] message) {
        try {
            Schema _schema = new Schema.Parser().parse(schemaStr);
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(_schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(message, null);
            GenericRecord result = reader.read(null, decoder);
            
            if (perf != null)
                perf.track(1, message.length);

            return result.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new String(message);
    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public byte[] serialize(String element) {
        try {
            return jsonToAvro(element, schemaStr);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return element.getBytes();
    }

    public TypeInformation<String> getProducedType (){
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

    public static byte[] jsonToAvro(String json, String schemaStr) throws IOException {
        InputStream input = null;
        GenericDatumWriter<GenericRecord> writer = null;
        Encoder encoder = null;
        ByteArrayOutputStream output = null;
        try {
            Schema schema = new Schema.Parser().parse(schemaStr);
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
