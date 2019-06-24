package kafkaproducerAvro;

import io.confluent.examples.clients.basicavro.Payment;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.commons.lang.SerializationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaAvroV1 {

    private static final String TOPIC = "transactions";
    public static void main(String[] args) {

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.112:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://192.168.0.112:8081");

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(props)) {

            for (long i = 0; i < 10; i++) {
                final String orderId = "id" + Long.toString(i);
                final Payment payment = new Payment(orderId, 1000.00d);
                final ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, "`1"," hello - " + i);
                producer.send(record);
//                Thread.sleep(1000L);
            }

            producer.flush();
            System.out.printf("Successfully produced 10 messages to a topic called %s%n", TOPIC);

        } catch (final SerializationException e) {
            e.printStackTrace();
        }

    }



//        try (KafkaProducer<String, Payment> producer = new KafkaProducer<>(props)) {
//
//            for (long i = 0; i < 10; i++) {
//                final String orderId = "id" + Long.toString(i);
//                final Payment payment = new Payment(orderId, 1000.00d);
//                final ProducerRecord<String, Payment> record = new ProducerRecord<String, Payment>(TOPIC, payment.getId().toString(), payment);
//                producer.send(record);
//                Thread.sleep(1000L);
//            }
//
//            producer.flush();
//            System.out.printf("Successfully produced 10 messages to a topic called %s%n", TOPIC);
//
//        } catch (final SerializationException e) {
//            e.printStackTrace();
//        } catch (final InterruptedException e) {
//            e.printStackTrace();
//        }
//
//    }

}
