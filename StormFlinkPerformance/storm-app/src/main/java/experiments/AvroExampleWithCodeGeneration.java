package experiments;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.LinkedHashMap;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.json.simple.JSONObject;

public class AvroExampleWithCodeGeneration {
	
	
	public void serialize() throws JsonParseException, JsonProcessingException, IOException {

		InputStream in = new FileInputStream("resources/StudentActivity.json");

		// create a schema
		Schema schema = new Schema.Parser().parse(new File("resources/StudentActivity.avsc"));
		// create an object to hold json record
		StudentActivity sa = new StudentActivity();
		// create an object to hold course_details
		Activity a = new Activity();
		
		// this file will have AVro output data
		File AvroFile = new File("resources/StudentActivity.avro");
		// Create a writer to serialize the record
		DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);		         
		DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

		dataFileWriter.create(schema, AvroFile);

		// iterate over JSONs present in input file and write to Avro output file
		for (Iterator it = new ObjectMapper().readValues(
				new JsonFactory().createJsonParser(in), JSONObject.class); it.hasNext();) {

			JSONObject JsonRec = (JSONObject) it.next();
			sa.setId( JsonRec.get("id").toString());
			sa.setStudent_id((int) JsonRec.get("student_id"));
			sa.setUniversity_id((Integer) JsonRec.get("university_id"));

			LinkedHashMap CourseDetails = (LinkedHashMap) JsonRec.get("course_details");
			a.setCourse_id((Integer) CourseDetails.get("course_id"));
			a.setEnroll_date( CourseDetails.get("enroll_date").toString());
			a.setVerb((String) CourseDetails.get("verb").toString());
			a.setResult_score((double) CourseDetails.get("result_score"));

			sa.setCourse_details(a);

			//dataFileWriter.append( sa);
		}  // end of for loop

		in.close();
		dataFileWriter.close();

	} // end of serialize method

	public void deserialize () throws IOException {
		// create a schema
		Schema schema = new Schema.Parser().parse(new File("resources/StudentActivity.avsc"));
		// create a record using schema
		GenericRecord AvroRec = new GenericData.Record(schema);
		File AvroFile = new File("resources/StudentActivity.avro");
		DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>(schema);
		DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(AvroFile, datumReader);
		System.out.println("Deserialized data is :");
		while (dataFileReader.hasNext()) {
			AvroRec = dataFileReader.next(AvroRec);
			System.out.println(AvroRec);
		}
	}

	public static void main(String[] args) throws JsonParseException, JsonProcessingException, IOException {
		AvroExampleWithCodeGeneration AvroEx = new AvroExampleWithCodeGeneration();
		//AvroEx.serialize();
		AvroEx.deserialize();
	}

}