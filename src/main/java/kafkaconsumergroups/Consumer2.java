package kafkaconsumergroups;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class Consumer2 {

    static final String TOPIC = "new-topic";
    static final String SERVERS="localhost:9092,localhost:9093,localhost:9094";


    public static void main(String[] args) {

        Properties properties = new Properties();
//        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.0.112:9092,192.168.0.112:9093,192.168.0.112:9094");
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group1");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        List<String> topicList = Arrays.asList(TOPIC);
        consumer.subscribe(topicList);
//          List<String> topicList = Arrays.asList("parti");
//          consumer.assign();

        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
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
