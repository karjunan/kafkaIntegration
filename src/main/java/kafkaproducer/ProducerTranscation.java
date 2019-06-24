package kafkaproducer;

import org.apache.kafka.clients.Metadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class ProducerTranscation {

    static final String TOPIC = "transaction-producer-topic";
    static final String TOPIC1 = "transaction-producer-topic-1";

    //    static final String SERVERS="localhost:9092,localhost:9093,localhost:9094";
    static final String SERVERS="192.168.0.115:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {

        Properties properties = new Properties();
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "transactional-id");

        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);

        producer.initTransactions();
        try {
            producer.beginTransaction();
            for (int i = 0; i < 20; i++){
                producer.send(new ProducerRecord<>(TOPIC, Integer.toString(i), Integer.toString(i)));
                producer.send(new ProducerRecord<>(TOPIC1, Integer.toString(i), Integer.toString(i)));
                Thread.sleep(1000);
                System.out.println("Inserted " + i);

            }

            producer.commitTransaction();
        } catch (ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            // We can't recover from these exceptions, so our only option is to close the producer and exit.
            System.out.println("Getting old tx IDS ");
//            producer.close();
        } catch (KafkaException e) {
            // For all other exceptions, just abort the transaction and try again.
            System.out.println("Trancation is not in good shape !! Aborting !!!");
            producer.abortTransaction();
//        }
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        producer.close();

    }
}


/*
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

 */