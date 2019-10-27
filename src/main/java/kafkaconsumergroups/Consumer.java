package kafkaconsumergroups;

import com.grpc.server.avro.Message;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Consumer {

    static final String TOPIC = "Topic1";
//    static final String SERVERS="localhost:9092,localhost:9093,localhost:9094";
    static final String SERVERS="pkc-4ygw7.ap-southeast-2.aws.confluent.cloud:9092";
    public static void main(String[] args) {

        Properties properties = new Properties();
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);

        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG,"group1");
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");


        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);

        List<String> topicList = Arrays.asList(TOPIC);
        consumer.subscribe(topicList);
//          List<String> topicList = Arrays.asList("parti");
//          consumer.assign();

        try {
            while(true) {
                ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String,String> record: records) {
                    System.out.println(record.key() + " ->  " + record.value() + " -> " + record.partition());
                }
            }

        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            consumer.close();
        }

    }

}
