package avro;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;

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

    }

}
