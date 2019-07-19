package kafkaproducerAvro;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.lang.SerializationException;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.IOException;
import java.util.Properties;

public class KafkaAvroV2 {

    private static final String TOPIC = "topic-1";
    public static void main(String[] args) throws IOException {

        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        try (KafkaProducer<String, GenericRecord> producer = new KafkaProducer<>(props)) {

            Schema.Parser parser = new Schema.Parser();
            Schema schema = parser.parse(new File("src\\main\\resources\\payment.avsc"));
            System.out.println(schema);
            GenericRecord avroRecord = new GenericData.Record(schema);
            System.out.println("Avro Record" + avroRecord);
            for (long i = 0; i < 10; i++) {
                final String orderId = "id" + Long.toString(i);
//                final Payment payment = new Payment(orderId, 1000.00d);
                    avroRecord.put("id","1000.00d");
//                final ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, "`1",payment);
                final ProducerRecord<String, GenericRecord> record = new ProducerRecord<String, GenericRecord>(TOPIC, "`1",avroRecord);

//                producer.send(record);
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
//        for (long i = 0; i < 10; i++) {
//            final String orderId = "id" + Long.toString(i);
//            final Payment payment = new Payment(orderId, 1000.00d);
////                final ProducerRecord<String, String> record = new ProducerRecord<String, String>(TOPIC, "`1",payment);
//            final ProducerRecord<String, Payment> record = new ProducerRecord<String, Payment>(TOPIC, "`1",payment);
//
//            producer.send(record);
////                Thread.sleep(1000L);
//        }
//
//        producer.flush();
//        System.out.printf("Successfully produced 10 messages to a topic called %s%n", TOPIC);
//
//    } catch (final SerializationException e) {
//        e.printStackTrace();
//    }

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
