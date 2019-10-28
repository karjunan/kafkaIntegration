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

public class CustomerComplexAvro {

    public static void main(String[] args) {

        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                "    \"type\" : \"record\",\n" +
                "    \"namespace\": \"com.example\",\n" +
                "    \"name\" : \"customer\",\n" +
                "    \"doc\" :\"Customer details defined\",\n" +
                "    \"fields\" : [\n" +
                "    {\"name\":\"firstName\", \"type\":\"string\", \"doc\":\"firstname of customer\"},\n" +
                "    {\"name\":\"middleName\",\"type\": [\"null\",\"string\"],\"default\": null, \"doc\" :\"middle name can be null and if present, take the name\"},\n" +
                "    {\"name\":\"lastName\", \"type\":\"string\", \"doc\":\"lastname of customer\"},\n" +
                "    {\"name\":\"age\", \"type\":\"int\", \"doc\":\"Age of customer\"},\n" +
                "    {\"name\":\"height\", \"type\":\"float\", \"doc\":\"height of customer\"},\n" +
                "    {\"name\":\"weight\", \"type\":\"float\", \"doc\":\"weight of customer in kgs\"},\n" +
                "    {\"name\":\"email\", \"type\":\"boolean\", \"default\":true,\"doc\":\"email featured enable or disabled\"},\n" +
                "    {\"name\":\"customerEmails\",\"type\": {\n" +
                "        \"type\":\"array\",\n" +
                "        \"items\":\"string\"\n" +
                "        }\n" +
                "        ,\"doc\":\"customer emails to be present , by default its null\", \"default\":[]},\n" +
                "    {\"name\":\"customerAddress\",\"default\":null, \"type\":[\"null\", \n" +
                "        {\n" +
                "            \"type\" : \"record\",\n" +
                "            \"namespace\": \"com.example\",\n" +
                "            \"name\" : \"customerAddress\",\n" +
                "            \"doc\" :\"Customer details defined\",\n" +
                "            \"fields\" : [\n" +
                "                {\"name\":\"street1\",\"type\":\"string\",\"doc\":\"Street where the customer lives\"},\n" +
                "                {\"name\":\"street2\",\"type\":\"string\",\"doc\":\"Street where the customer lives\"},\n" +
                "                {\"name\":\"state\",\"type\":\"string\",\"doc\":\"state where the customer lives\"},\n" +
                "                {\"name\":\"country\",\"type\":\"string\",\"doc\":\"country where the customer lives\"},\n" +
                "                {\"name\":\"zipcode\",\"type\":\"int\",\"doc\":\"zip code of the area\"}\n" +
                "            ]\n" +
                "            \n" +
                "        }]    \n" +
                "    }\n" +
                "    ]\n" +
                "}\n");
        System.out.println(schema);

    }

}
