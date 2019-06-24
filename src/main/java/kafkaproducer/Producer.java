package kafkaproducer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class Producer {

    static final String TOPIC = "stream-name-producer-topic";
//    static final String SERVERS="localhost:9092,localhost:9093,localhost:9094";
    static final String SERVERS="192.168.0.115:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {

        Properties properties = new Properties();
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);

        try{

            ProducerRecord<String,String> producerRecord = null;
//            String str = "current temp current";
            List<String> list = new ArrayList<>();
            list.add("KRishna");
            list.add("Lokesh");
            list.add("Maddy");
            for( String str : list) {
//                new ProducerRecord<>("topic",p)
                producerRecord = new ProducerRecord(TOPIC,str);
                producer.send(producerRecord);
            System.out.println(str);
//                System.out.println("Sending records -> " + i + " - " + producerRecord.key() + " - " + producerRecord.value() );
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            producer.close();
        }

    }
}
