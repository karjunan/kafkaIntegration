package kafkaStreams;

import kafkaStreams.processors.UserNameExtractor;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.state.Stores;

import java.util.Properties;

public class StreamTopology {

    static final String NAME_TOPIC = "stream-name-producer-topic2";
//    static final String ADDRESS_TOPIC = "stream-address-producer-topic";
//    static final String AGE_TOPIC = "stream-age-producer-topic";

    static final String SERVERS="localhost:9092";

    public static void main(String[] args) {

        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "name-read-2");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = new Topology();

        topology.addSource("SOURCE1",NAME_TOPIC)
                .addProcessor("PROCESS1" , UserNameExtractor::new,"SOURCE1")
                .addStateStore(Stores.keyValueStoreBuilder(Stores.persistentKeyValueStore("Names1"),
                        Serdes.String(),Serdes.String()))
                .addSink("SINK1","good-user-topic2","PROCESS1");

        KafkaStreams streams = new KafkaStreams(topology,properties);
        streams.start();

    }
}
