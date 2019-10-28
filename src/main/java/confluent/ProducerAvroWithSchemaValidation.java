package confluent;

import com.example.Customer;
import io.confluent.examples.clients.cloud.DataRecordAvro;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProducerAvroWithSchemaValidation {

    // Create topic in Confluent Cloud
    public static void createTopic(final String topic,
                                   final int partitions,
                                   final int replication,
                                   final Properties cloudConfig) {
        final NewTopic newTopic = new NewTopic(topic, partitions, (short) replication);
        try (final AdminClient adminClient = AdminClient.create(cloudConfig)) {
            adminClient.createTopics(Collections.singletonList(newTopic)).all().get();
        } catch (final InterruptedException | ExecutionException e) {
            // Ignore if TopicExistsException, which may be valid if topic exists
            if (!(e.getCause() instanceof TopicExistsException)) {
                throw new RuntimeException(e);
            }
        }
    }

    public static void main(final String[] args) throws IOException {
        Properties properties = new Properties();
        properties = loadConfig(args[0]);

        // Create topic if needed
        final String topic = "t1";
        System.out.println(properties);
//        createTopic(topic, 1, 1, properties);

        // Add additional properties.
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
//        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

//        Producer<String, DataRecordAvro> producer = new KafkaProducer<String, DataRecordAvro>(properties);
        Producer<String, String> producer = new KafkaProducer(properties);
        // Produce sample data
        final Long numMessages = 10L;

        GenericRecord record = buildRecord();

        for (Long i = 0L; i < numMessages; i++) {

//            System.out.printf("Producing record: %s\t%s%n", customer);
            producer.send(new ProducerRecord(topic, "Hello"), new Callback() {
                @Override
                public void onCompletion(RecordMetadata m, Exception e) {
                    if (e != null) {
                        e.printStackTrace();
                    } else {
                        System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
                    }
                }
            });
        }


        producer.flush();

        System.out.printf("10 messages were produced to topic %s%n", topic);

        producer.close();
    }

    public static Properties loadConfig(final String configFile) throws IOException {
        if (!Files.exists(Paths.get(configFile))) {
            throw new IOException(configFile + " not found.");
        }
        final Properties cfg = new Properties();
        try (InputStream inputStream = new FileInputStream(configFile)) {
            cfg.load(inputStream);
        }
        return cfg;
    }

    public static GenericRecord buildRecord() throws IOException {
        String schemaPath = "integrationevent.avsc";
//        System.out.println();
        Stream<String> schemaString = Files.lines(Paths.get("src","main","resources","message.avsc"));
        String result = schemaString.collect(Collectors.joining(" "));
        System.out.println("result =>" + result);
        Schema schema = new Schema.Parser().parse(result);
        GenericData.Record record = new GenericData.Record(schema);
        return record;
    }




}