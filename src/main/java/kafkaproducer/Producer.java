package kafkaproducer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.messages.Employee;
import com.messages.Employees;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;

public class Producer {

    static final String TOPIC = "employee-topic-1";
    static final String SERVERS="10.0.102.166:9092";
//    static final String SERVERS="192.168.0.133:9092";
//    static final String SERVERS="172.24.235.1:9092";

    // static final String SERVERS="192.168.0.115:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {

        Properties properties = new Properties();
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,KafkaAvroSerializer.class.getName());

        System.out.println(properties);

        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);

        try{

            ProducerRecord<String,String> producerRecord = null;
            Random random = new Random();
            for( int i = 0; i < 1 ;i++) {
                for (Employee e : Employees.getEmployees()) {
                    e.setId(random.nextInt(1000));
                    ObjectMapper Obj = new ObjectMapper();
                    String jsonStr = Obj.writeValueAsString(e);
                    producerRecord = new ProducerRecord<>(TOPIC,jsonStr );
                    producer.send(producerRecord);
                    System.out.println(producerRecord.toString());

//                System.out.println("Sending records -> " + i + " - " + producerRecord.key() + " - " + producerRecord.value() );
                }
//                Thread.sleep(10000);
            }


//                new ProducerRecord<>("topic",p)

        } catch (Exception ex) {
            System.out.println(ex.getMessage());
        } finally {
            producer.close();
        }

    }
}
