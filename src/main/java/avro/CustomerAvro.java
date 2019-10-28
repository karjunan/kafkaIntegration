package avro;

import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DatumWriter;

import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;

public class CustomerAvro {


    public static void main(String[] args) {

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                "    \"type\" : \"record\",\n" +
                "    \"namespace\": \"com.example\",\n" +
                "    \"name\" : \"customerAddress\",\n" +
                "    \"doc\" :\"Customer details defined\",\n" +
                "    \"fields\" : [\n" +
                "        {\"name\":\"street1\",\"type\":\"string\",\"doc\":\"Street where the customer lives\"},\n" +
                "        {\"name\":\"street2\",\"type\":\"string\",\"doc\":\"Street where the customer lives\"},\n" +
                "        {\"name\":\"state\",\"type\":\"string\",\"doc\":\"state where the customer lives\"},\n" +
                "        {\"name\":\"country\",\"type\":\"string\",\"doc\":\"country where the customer lives\"},\n" +
                "        {\"name\":\"zipcode\",\"type\":\"int\",\"doc\":\"zip code of the area\"}\n" +
                "    ]\n" +
                "    \n" +
                "}");

        GenericRecordBuilder builder = new GenericRecordBuilder(schema);
        builder.set("street1","hinkal")
                .set("street2","mysore")
                .set("state","karnataka")
                .set("country","india")
                .set("zipcode",570017);

        System.out.println(builder.build().toString());

        File file = new File("customer.avro");

        writeData(schema,builder.build(),file);

        readData(file);

    }

    private static void readData(File file) {
        final DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();

        try(DataFileReader<GenericRecord> fileReader = new DataFileReader<GenericRecord>(file,datumReader)) {
            fileReader.forEachRemaining(v -> System.out.println(v.toString()));
            System.out.println("Completed reading from avro file !!");
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }


    private static void writeData(Schema schema, GenericRecord record, File file) {
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(schema);
        try(DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter)) {

            dataFileWriter.create(schema,file);
            dataFileWriter.append(record);
            System.out.println("Customer data written to a file !!");

        } catch ( Exception ex) {
            ex.printStackTrace();
        }

    }


}
