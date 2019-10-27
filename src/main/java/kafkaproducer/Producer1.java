package kafkaproducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.Future;

public class Producer1 {

    static final String TOPIC = "topic1";

    static final String SERVERS="pkc-4ygw7.ap-southeast-2.aws.confluent.cloud:9092";
    //    static final String SERVERS="192.168.56.1:29092";
//    static final String SERVERS="172.24.23    5.1:9092";

    // static final String SERVERS="192.168.0.115:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) throws IOException {

        Properties properties = new Properties();
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        properties.put ("ssl.endpoint.identification.algorithm","https");
        properties.put ("sasl.mechanism","PLAIN");
//        properties.put ("request.timeout.ms",2000);
        properties.put ("retry.backoff.ms",500);
//        properties.put ("ssl.endpoint.identification.algorithm","500");
//        properties.put ("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"VCKGRKYRAZ6KLY5I\" password=\"kE1FrjbRhB9ZCDdrv7VoOVTQ3AiHIUsWVYXa2ZU19jbGCue/b9WqcHEwm9XaYjd1\"\\;");
            properties.put("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"BSWROXTOGPRX5WST\" password=\"6j5/XXePrnkzpartmhX4qM5qEmWpseEY3l5rIiMvRB8Yc6iVNrZR/LYv+By9/jaU\";");

        properties.put("security.protocol","SASL_SSL");

        System.out.println(properties);

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        try {
            ProducerRecord<String, String> producerRecord = null;

            Random random = new Random();
            for (int i = 15; i < 20; i++) {
                producerRecord = new ProducerRecord(TOPIC, "Key - "+ new Random().nextInt(1000),"Test topic " + 105+i);
                Headers headers = producerRecord.headers();
                Header header1 = new RecordHeader("Named Key","value".getBytes());
                headers.add(header1);
                Future<RecordMetadata> metadata = producer.send(producerRecord);

//                RecordMetadata rc = metadata.get();
//                System.out.println(producerRecord.toString() + " = >  " + rc.offset() +  "=> " +rc.partition()
//                + "=> " + rc.topic() );
                producer.flush();
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            producer.close();
        }
    }

}
