package kafkaproducerAvro;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.subject.TopicRecordNameStrategy;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;

import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaAvroTopicRecordNameStratergy {

    private static final String TOPIC = "t1";

    public static void main(String[] args) {
//        io.confluent.kafka.serializers.subject.TopicRecordNameStrategy

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put("value.subject.name.strategy", TopicRecordNameStrategy.class.getName());


        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {

            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse("{\n" +
                    "    \"type\" : \"record\",\n" +
                    "    \"namespace\": \"com.example\",\n" +
                    "    \"name\" : \"customerAddress\",\n" +
                    "    \"doc\" :\"Customer details defined\",\n" +
                    "    \"fields\" : [\n" +
                    "        {\"name\":\"street1\",\"type\":\"string\",\"doc\":\"Street where the customer lives\"},\n" +
                    "        {\"name\":\"street2\",\"type\":[\"null\",\"string\"],\"doc\":\"Street where the customer lives\", \"default\":null},\n" +
                    "        {\"name\":\"country\",\"type\":\"string\",\"doc\":\"country where the customer lives\"},\n" +
                    "        {\"name\":\"zipcode\",\"type\":\"int\",\"doc\":\"zip code of the area\"}\n" +
                    "    ]\n" +
                    "    \n" +
                    "}");

//            customerCreated
//            customerDetails
//            customerAddress

            String data = "{\"street1\": \"hinkal - 0\", \"street2\": \"mysore - 1\", \"country\": \"india\", \"zipcode\": 570017}";
            for(int i = 0 ; i < 5 ; i++) {
//                GenericRecordBuilder builder = new GenericRecordBuilder(schema);
//                builder.set("street1","hinkal - " + i)
//                        .set("street2","mysore")
////                        .set("state","karnataka")
//                        .set("country","india")
//                        .set("zipcode",570017);

                GenericRecord record = new JsonAvroConverter().convertToGenericDataRecord(data.getBytes(),schema);

                System.out.println("Looping data " + i);
                System.out.println(record);
                Future<RecordMetadata> future = producer.send(new ProducerRecord<>(TOPIC,record));
                System.out.println(future.get());

            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }
}
