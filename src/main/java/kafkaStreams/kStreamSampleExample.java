package kafkaStreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

public class kStreamSampleExample {

    static final String NAME_TOPIC = "stream-name-producer-topic";
    static final String ADDRESS_TOPIC = "stream-address-producer-topic";
    static final String AGE_TOPIC = "stream-age-producer-topic";

    static final String SERVERS="localhost:9092,localhost:9093,localhost:9094";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"name-read-example");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,SERVERS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());

        StreamsBuilder nameAgeBuilder = new StreamsBuilder();
        StreamsBuilder addressBuilder = new StreamsBuilder();

//        List<String> list = new ArrayList<>();
//        list.add(NAME_TOPIC);
//        list.add(AGE_TOPIC);
        KStream<String,String> name = nameAgeBuilder.stream(NAME_TOPIC);
//        KStream<String,String> address = addressBuilder.stream(ADDRESS_TOPIC);

        KTable<String,Long> table = name.mapValues(v -> v.toLowerCase())
                .flatMapValues(values -> Arrays.asList(values.split(" ")))
                .selectKey((key,value) -> value)
                .groupByKey()
                .count();

        table.toStream().foreach((k,v) -> System.out.println( k + " - " + v));

        KafkaStreams kafkaStreams = new KafkaStreams(nameAgeBuilder.build(),properties);
        kafkaStreams.start();
        System.out.println("Finally done");
    }
}
