package confluent;

import io.confluent.examples.clients.basicavro.Payment;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import scala.collection.immutable.Stream;

import java.util.Collections;
import java.util.Properties;

public class ConfluentConsumer {

    static final String TOPIC = "t2";
    static final String SERVERS="pkc-4ygw7.ap-southeast-2.aws.confluent.cloud:9092";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "t1");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put("ssl.endpoint.identification.algorithm","https");
        properties.put("sasl.mechanism","PLAIN");
        properties.put("request.timeout.ms","1000");
        properties.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"HR7BMQFCH3FCA32I\" password=\"AFI+3XWz0f+JIF983A05Qy+T8G7i0GDyqLO//WDUPIR0pEYyrWK8u6w4gGgiuC1S\";");
        properties.put("security.protocol","SASL_SSL");
        properties.put("basic.auth.credentials.source","USER_INFO");
//        properties.put("basic.auth.credentials.source","USER_INFO");

        try (final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(Collections.singletonList(TOPIC));

            while (true) {
//                System.out.println("Listening on => "+ TOPIC);
                final ConsumerRecords<String, String> records = consumer.poll(100);
                for (final ConsumerRecord<String, String> record : records) {
                    System.out.println(record.key() +" = " + record.value());
                }
            }

        } catch (Exception ex) {
            System.out.println(ex);
        }

    }
}
