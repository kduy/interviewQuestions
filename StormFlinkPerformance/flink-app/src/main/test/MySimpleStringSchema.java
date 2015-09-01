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

public class MySimpleStringSchema implements SerializationSchema<String, byte[]>, DeserializationSchema<String> {

    @Override
    public String deserialize(byte[] message) {
        try {
            Schema _schema = new Schema.Parser().parse(new File("/tmp/message.avsc"));
            DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(_schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(message, null);
            GenericRecord result = reader.read(null, decoder);
            System.out.println(result.toString());
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
        //return  element.getBytes();
        try {
            return jsonToAvro(element, "/tmp/message.avsc");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return element.getBytes();
    }

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

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }
}
