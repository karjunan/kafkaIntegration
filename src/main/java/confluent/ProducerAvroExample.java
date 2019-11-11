package confluent;

import com.example.Customer;
import io.confluent.examples.clients.cloud.DataRecordAvro;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
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

public class ProducerAvroExample {

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
        final String SERVERS="pkc-4ygw7.ap-southeast-2.aws.confluent.cloud:9092";
        // Load properties from a configuration file
        // The configuration properties defined in the configuration file are assumed to include:
        Properties properties = new Properties();
        properties = loadConfig(args[0]);
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
//        properties.put("ssl.endpoint.identification.algorithm","https");
//        properties.put("sasl.mechanism","PLAIN");
//        properties.put("request.timeout.ms","2000");
//        properties.put("retry.backoff.ms","500");
//        properties.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"HR7BMQFCH3FCA32I\" password=\"AFI+3XWz0f+JIF983A05Qy+T8G7i0GDyqLO//WDUPIR0pEYyrWK8u6w4gGgiuC1S\";");
//        properties.put("security.protocol","SASL_SSL");
//        properties.put("heartbeat.interval.ms", "2000");
//        properties.put("auto.commit.interval.ms", "1000");
//        properties .put("auto.offset.reset", "earliest");
//        properties.put(ProducerConfig.ACKS_CONFIG, "all");
//        properties.put(ProducerConfig.RETRIES_CONFIG, 0);
//        properties.put("basic.auth.credentials.source","USER_INFO");
//        properties.put("schema.registry.url", "https://psrc-l6oz3.us-east-2.aws.confluent.cloud");
//        properties.put("schema.registry.basic.auth.user.info" ,"PBDH6NJ4XFQRTJPS:MQFEt01i9ECjMMgsVAnSdHvXrRQgU+sTBNLmmiUviVxtHAadM8uLOB9VboVfLUau");
//           ssl.endpoint.identification.algorithm=https
//           sasl.mechanism=PLAIN
//           request.timeout.ms=20000
//           bootstrap.servers=<CLUSTER_BOOTSTRAP_SERVER>
//           retry.backoff.ms=500
//           sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<CLUSTER_API_KEY>" password="<CLUSTER_API_SECRET>";
//           security.protocol=SASL_SSL
//           basic.auth.credentials.source=USER_INFO
//           schema.registry.basic.auth.user.info=<SR_API_KEY>:<SR_API_SECRET>
//           schema.registry.url=https://<SR ENDPOINT>
//        final Properties props = loadConfig(args[0]);

        // Create topic if needed
        final String topic = "t6";
        createTopic(topic, 1, 3, properties);

        // Add additional properties.
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
//        properties.put(AbstractKafkaAvroSerDeConfig.AUTO_REGISTER_SCHEMAS, false);

        Producer<String, DataRecordAvro> producer = new KafkaProducer<String, DataRecordAvro>(properties);

        // Produce sample data
        final Long numMessages = 10L;
        Schema.Parser parser = new Schema.Parser();
//        Schema schema = parser.parse("{\n" +
//                "     \"type\": \"record\",\n" +
//                "     \"namespace\": \"com.example\",\n" +
//                "     \"name\": \"Customer\",\n" +
//                "     \"fields\": [\n" +
//                "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
//                "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" +
//                "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
//                "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
//                "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
//                "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" }\n" +
//                "     ]\n" +
//                "}");

        Schema schema = parser.parse("{\n" +
                "     \"type\": \"record\",\n" +
                "     \"namespace\": \"com.example\",\n" +
                "     \"name\": \"Customer\",\n" +
                "     \"fields\": [\n" +
                "       { \"name\": \"first_name\", \"type\": \"string\", \"doc\": \"First Name of Customer\" },\n" +
                "       { \"name\": \"last_name\", \"type\": \"string\", \"doc\": \"Last Name of Customer\" },\n" +
                "       { \"name\": \"age\", \"type\": \"int\", \"doc\": \"Age at the time of registration\" },\n" +
                "       { \"name\": \"height\", \"type\": \"float\", \"doc\": \"Height at the time of registration in cm\" },\n" +
                "       { \"name\": \"weight\", \"type\": \"float\", \"doc\": \"Weight at the time of registration in kg\" },\n" +
                "       { \"name\": \"automated_email\", \"type\": \"boolean\", \"default\": true, \"doc\": \"Field indicating if the user is enrolled in marketing emails\" }\n" +
                "     ]\n" +
                "}");

        GenericRecordBuilder customerBuilder = new GenericRecordBuilder(schema);
        customerBuilder.set("first_name", "John");
        customerBuilder.set("last_name", "Doe");
        customerBuilder.set("age", 26);
        customerBuilder.set("height", 175f);
        customerBuilder.set("weight", 70.5f);
        customerBuilder.set("automated_email", false);
        GenericData.Record myCustomer = customerBuilder.build();
        System.out.println("Customer >>>>>>>>>>>>>>>>>>>>>>>>>  " + myCustomer);

        Customer customer = Customer.newBuilder()
                .setAge(10).setEmail("abd").setFirstName("Krishna").setLastName("Arjunan").setHeight(173f).setWeight(70.0f).setPhoneNumber("123123123").build();
        for (Long i = 0L; i < numMessages; i++) {
            String key = "Rock";

//            System.out.printf("Producing record: %s\t%s%n", customer);
            producer.send(new ProducerRecord(topic,  key,myCustomer), new Callback() {
                new ProducerRecord
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

//        final Long numMessages = 10L;
//        for (Long i = 0L; i < numMessages; i++) {
//            String key = "alice";
//            DataRecordAvro record = new DataRecordAvro(i);
//
////        GenericRecordBuilder record = new GenericRecordBuilder(DataRecordAvro.SCHEMA$);
////        record.set("count",1l);
////        record.build();
//            System.out.printf("Producing record: %s\t%s%n", key, record);
////        producer.send(new ProducerRecord<String, GenericRecord>(topic, key,  record.build()), new Callback() {
//
//            producer.send(new ProducerRecord<String, DataRecordAvro>(topic, key, record), new Callback() {
//                @Override
//                public void onCompletion(RecordMetadata m, Exception e) {
//                    if (e != null) {
//                        e.printStackTrace();
//                    } else {
//                        System.out.printf("Produced record to topic %s partition [%d] @ offset %d%n", m.topic(), m.partition(), m.offset());
//                    }
//                }
//            });
//        }


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




}