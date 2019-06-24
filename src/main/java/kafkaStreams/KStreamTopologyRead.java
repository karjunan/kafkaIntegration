package kafkaStreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

public class KStreamTopologyRead {

    static final String NAME_TOPIC = "good-user-topic";

    static final String SERVERS="localhost:9092";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"name-read-examples"+new Random().nextInt());
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,SERVERS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        try{
            StreamsBuilder nameAgeBuilder = new StreamsBuilder();

            KStream<String,String> name = nameAgeBuilder.stream(NAME_TOPIC);

            name.mapValues(v -> v.toLowerCase()).foreach((k,v) -> System.out.println(k  + " =  " + v));
//        KTable<String,Long> table = name.mapValues(v -> v.toLowerCase())
//                                        .selectKey((key,value) -> value)
//                                        .groupByKey()
//                                          .count();

//        table.toStream().foreach((k,v) -> System.out.println(k + " - - " + v));

            KafkaStreams kafkaStreams = new KafkaStreams(nameAgeBuilder.build(),properties);
            kafkaStreams.start();
        }catch (Exception ex) {
            System.out.println(ex);
        }

    }
}
