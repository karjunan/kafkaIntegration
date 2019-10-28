package confluent;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import tech.allegro.schema.json2avro.converter.JsonAvroConverter;
import tech.allegro.schema.json2avro.converter.JsonGenericRecordReader;

import javax.print.DocFlavor;
import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ConfluentProducerAvroSimpleLocal {

    static final String TOPIC = "t2";
    static final String SERVERS="http://172.30.242.65:9092";

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
//        properties.put("value.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");
//        properties.put("ssl.endpoint.identification.algorithm","https");
//        properties.put("sasl.mechanism","PLAIN");
//        properties.put("request.timeout.ms","2000");
//        properties.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"HR7BMQFCH3FCA32I\" password=\"AFI+3XWz0f+JIF983A05Qy+T8G7i0GDyqLO//WDUPIR0pEYyrWK8u6w4gGgiuC1S\";");
//        properties.put("security.protocol","SASL_SSL");
//        properties.put("heartbeat.interval.ms", "2000");
//        properties.put("auto.commit.interval.ms", "1000");
        properties .put("auto.offset.reset", "earliest");
//        properties.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
//        properties.put("value.deserializer", io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
//        properties.put(SCHEMA_REGISTRY_URL, restApp.restConnect);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                KafkaAvroSerializer.class);

//        properties.put(AbstractKafkaAvroSerDeConfig.A, false);

//        properties.put("basic.auth.credentials.source","USER_INFO");
        properties.put("schema.registry.url", "http://172.30.242.65:8081");
//        properties.put("schema.registry.url", "https://psrc-l6oz3.us-east-2.aws.confluent.cloud:443");
        properties.put("schema.registry.basic.auth.user.info" ,"PBDH6NJ4XFQRTJPS:MQFEt01i9ECjMMgsVAnSdHvXrRQgU+sTBNLmmiUviVxtHAadM8uLOB9VboVfLUau");

        KafkaProducer<String,Object> producer = new KafkaProducer<>(properties);
//        KafkaProducer<String, GenericRecord> producer = new KafkaProducer(properties);
        System.out.println(properties);
//        IndexedRecord record = createAvroRecord();
        ProducerRecord<String, Object> psRecprd;
        for(int i = 0 ; i < 5 ; i++) {
            System.out.println("Looping data " + i);
            Future<RecordMetadata> future = producer.send(new ProducerRecord(TOPIC,null));
            System.out.println(future.get());

        }
    }

//    public static GenericRecord createAvroRecord() {
//        String userSchema = "{\"namespace\": \"example.avro\", \"type\": \"record\", " +
//                "\"name\": \"user\"," +
//                "\"fields\": [{\"name\": \"name\", \"type\": \"string\"}," +
//                             "{\"name\": \"age\", \"type\": \"string\", \"default\": \"0\"}]}";
//
//
//
//        Schema.Parser parser = new Schema.Parser();
//        Schema schema = parser.parse(userSchema);


//        String data = "{\"name\": \"Krishna\" ," +
//                       "\"age\": \"10\"}";
//        String data = "{\"name\": \"Krishna\"}";

//        GenericRecord avroRecord = new JsonAvroConverter().convertToGenericDataRecord(data.getBytes(),schema);
//        GenericRecord avroRecord = new GenericData.Record(schema);
//        avroRecord.put("name","krishna");
//        avroRecord.put("age","100");
//        System.out.println(avroRecord.toString());
//        JsonAvroConverter converter = new JsonAvroConverter();
//        byte[] bytes = converter.convertToAvro(data.getBytes(), userSchema);
//        avroRecord.put("name", "Krishna");

//        return avroRecord;
//    }
}
