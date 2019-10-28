package confluent;

import org.apache.kafka.clients.MetadataUpdater;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;

public class ConfluentProducer {

    static final String TOPIC = "t2";
    static final String SERVERS="pkc-4ygw7.ap-southeast-2.aws.confluent.cloud:9092";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put("ssl.endpoint.identification.algorithm","https");
        properties.put("sasl.mechanism","PLAIN");
        properties.put("request.timeout.ms","1000");
        properties.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"HR7BMQFCH3FCA32I\" password=\"AFI+3XWz0f+JIF983A05Qy+T8G7i0GDyqLO//WDUPIR0pEYyrWK8u6w4gGgiuC1S\";");
        properties.put("security.protocol","SASL_SSL");
        properties.put("basic.auth.credentials.source","USER_INFO");
//        properties.put("basic.auth.credentials.source","USER_INFO");

        KafkaProducer<String, String> producer = new KafkaProducer(properties);
        System.out.println(properties);
        try {

            for (int i = 0; i < 1000000000; i++) {
                   Future<RecordMetadata> future = producer.send(new ProducerRecord<>("t2","Testing ksql"));
                System.out.println("Message Sent " + future.get().offset());
            }

        } catch (Exception ex ) {
            System.out.println(ex);

        }

    }
}
