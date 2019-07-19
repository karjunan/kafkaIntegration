package kafkaStreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;
import org.graalvm.polyglot.Context;

import java.util.Arrays;
import java.util.Properties;


public class kStreamSampleExample {

    static final String NAME_TOPIC = "name-read-example-2";
    static final String MODIFIED_NAME_TOPIC = "stream-modified-name-producer-topic";
    static final String SERVERS="localhost:9092,localhost:9093,localhost:9094";
    Properties properties;

    public static void main(String[] args) {
        kStreamSampleExample ks = new kStreamSampleExample();
        ks.init();
        ks.startProcessing();
        System.out.println(" All Properties => " + ks.properties);
    }

    public void init() {
        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"name-read-example2");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,SERVERS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
    }

    public KStream startProcessing() {

        StreamsBuilder nameAgeBuilder = new StreamsBuilder();
        KStream<String,String> name = nameAgeBuilder.stream(NAME_TOPIC);

        KTable<String,Long> table = name.mapValues(v -> v.toLowerCase())
                .flatMapValues(values -> Arrays.asList(values.split(" ")))
                .selectKey((key,value) -> value)
                .groupByKey()
                .count();

        name.foreach((k,v) -> System.out.println("key " + k + " = " + v));
        System.out.println(" table data created => " + table);
        table.toStream().foreach((k,v) -> System.out.println( k + " - " + v));
        table.toStream().to(MODIFIED_NAME_TOPIC,  Produced.with(Serdes.String(), Serdes.Long()));
        KafkaStreams kafkaStreams = new KafkaStreams(nameAgeBuilder.build(),properties);
        kafkaStreams.start();
        System.out.println("Finally done");
        return name;
    }


}
