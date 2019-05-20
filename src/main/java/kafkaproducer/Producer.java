package kafkaproducer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());


        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);

        try{

            ProducerRecord<String,String> producerRecord = null;
            for( int i = 0 ; i < 10 ; i++ ) {
                String str = " Message " + i;
//                new ProducerRecord<>("topic",p)
                producerRecord = new ProducerRecord("my-topic",str);
                producer.send(producerRecord);
                System.out.println("Sending records -> " + i);
            }
        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            producer.close();
        }

    }
}
