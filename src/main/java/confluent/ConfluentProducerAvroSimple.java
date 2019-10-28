package confluent;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.generic.IndexedRecord;
import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ConfluentProducerAvroSimple {

    static final String TOPIC = "t2";
    static final String SERVERS="pkc-4ygw7.ap-southeast-2.aws.confluent.cloud:9092";

    public static void main(String[] args) throws IOException, InterruptedException, ExecutionException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
//        properties.put("value.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");
        properties.put("ssl.endpoint.identification.algorithm","https");
        properties.put("sasl.mechanism","PLAIN");
        properties.put("request.timeout.ms","2000");
        properties.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"HR7BMQFCH3FCA32I\" password=\"AFI+3XWz0f+JIF983A05Qy+T8G7i0GDyqLO//WDUPIR0pEYyrWK8u6w4gGgiuC1S\";");
        properties.put("security.protocol","SASL_SSL");
        properties.put("heartbeat.interval.ms", "2000");
        properties.put("auto.commit.interval.ms", "1000");
        properties .put("auto.offset.reset", "earliest");
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
//        properties.put("key.deserializer", org.apache.kafka.common.serialization.StringDeserializer.class);
//        properties.put("value.deserializer", io.confluent.kafka.serializers.KafkaAvroDeserializer.class);
//        properties.put(SCHEMA_REGISTRY_URL, restApp.restConnect);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                org.apache.kafka.common.serialization.StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                io.confluent.kafka.serializers.KafkaAvroSerializer.class.getName());

        properties.put("basic.auth.credentials.source","USER_INFO");
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "https://psrc-l6oz3.us-east-2.aws.confluent.cloud");
        properties.put("schema.registry.basic.auth.user.info" ,"PBDH6NJ4XFQRTJPS:MQFEt01i9ECjMMgsVAnSdHvXrRQgU+sTBNLmmiUviVxtHAadM8uLOB9VboVfLUau");

        KafkaProducer<String,Object> producer = new KafkaProducer<>(properties);
//        KafkaProducer<String, GenericRecord> producer = new KafkaProducer(properties);
        System.out.println(properties);
//        IndexedRecord record = createAvroRecord();
        ProducerRecord<String, Object> psRecprd;
        String data = "{\"id\":\"Ksql working buddy\"}";
        for(int i = 0 ; i < 100 ; i++) {
            Future<RecordMetadata> future = producer.send(new ProducerRecord<>(TOPIC, data));
            System.out.println(future.get().toString());
        }
    }

//    public static IndexedRecord createAvroRecord() {
//        String userSchema = "{\"namespace\": \"com.pickles.t3\", \"type\": \"record\", " +
//                "\"name\": \"value_t3\"," +
//                "\"fields\": [{\"name\": \"id\", \"type\": \"string\"}]}";

//        String userSchema = "{\"type\": \"record\",\"name\": \"T2\",\"namespace\": \"com.pickles.schema.t2\",\"fields\": [{\"name\": \"id\",\"type\": \"string\"},{\"name\": \"amount\",\"type\": \"double\"}]}";
//     String userSchema = "{\"namespace\": \"com.pickles.schema.t2\",\n" +
//             " \"type\": \"record\",\n" +
//             " \"name\": \"T2\",\n" +
//             " \"fields\": [\n" +
//             "     {\"name\": \"id\", \"type\": \"string\"},\n" +
//             "     {\"name\": \"amount\", \"type\": \"double\"},\n" +
//             "     {\"name\": \"region\", \"type\": \"string\"}\n" +
//             " ]\n" +
//             "}";

//        Schema.Parser parser = new Schema.Parser();
//        Schema schema = parser.parse(userSchema);
//        GenericRecord avroRecord = new GenericData.Record(schema);
//        avroRecord.put("id", "Krishna");
//        avroRecord.put("amount",10d);
//        avroRecord.put("region","india");

//        return avroRecord;
//    }
}
