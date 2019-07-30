package kafkaStreams;

import com.grpc.server.avro.Message;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class kStreamSampleExample_GenericRecord {

    static final String NAME_TOPIC = "t1";
    static final String MODIFIED_NAME_TOPIC = "stream-modified-name-producer-topic";
    static final String SERVERS="localhost:9092,localhost:9093,localhost:9094";
    Properties properties;

    public static void main(String[] args) {
        kStreamSampleExample_GenericRecord ks = new kStreamSampleExample_GenericRecord();
        ks.init();
        ks.startProcessing();
        System.out.println(" All Properties => " + ks.properties);
    }

    public void init() {
        properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG,"name-read-example2");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,SERVERS);
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

    }

    public KStream startProcessing() {

        StreamsBuilder nameAgeBuilder = new StreamsBuilder();
        KStream<String,byte[]> name = nameAgeBuilder.stream(NAME_TOPIC);

        name.foreach((k,v) -> {
            byte[] received_message = v;
            System.out.println(received_message);
            Schema schema = null;
            schema = new Schema.Parser().parse(getAvroData());
            DatumReader<GenericRecord> reader = new SpecificDatumReader<>(schema);
            Decoder decoder = DecoderFactory.get().binaryDecoder(received_message, null);
            GenericRecord payload2 = null;
            try {
                payload2 = reader.read(null, decoder);
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("Message received : " + payload2);
        });

        name.foreach((k,v) -> System.out.println("key " + k + " = " + v));
//        System.out.println(" table data created => " + table);
//        table.toStream().foreach((k,v) -> System.out.println( k + " - " + v));
//        table.toStream().to(MODIFIED_NAME_TOPIC,  Produced.with(Serdes.String(), Serdes.Long()));
        KafkaStreams kafkaStreams = new KafkaStreams(nameAgeBuilder.build(),properties);
        kafkaStreams.start();
        System.out.println("Finally done");
        return name;
    }

    public static String getAvroData() {

        try {
            return Files.lines(Paths.get("src", "main", "resources", "message.avsc"))
                    .collect(Collectors.toList()).stream().collect(Collectors.joining(" "));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }
}
