package kafkaproducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

public class ProducerForTopology {

    static final String TOPIC = "stream-name-producer-topic2";
    static final String SERVERS="localhost:9092";

    public static void main(String[] args) {

        Properties properties = new Properties();
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);

        try{

            ProducerRecord<String,String> producerRecord = null;
            List<String> list = new ArrayList<>();
            list.add("Ram");
            list.add("Ankish");
            list.add("rony");
            for( String str : list) {
                producerRecord = new ProducerRecord(TOPIC,str);
                producer.send(producerRecord);
            System.out.println(str);
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            producer.close();
        }

    }
}
