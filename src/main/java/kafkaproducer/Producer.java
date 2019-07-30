package kafkaproducer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.grpc.server.avro.Message;
import com.messages.Employee;
import com.messages.Employees;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class Producer {

//    static final String TOPIC = "t5";

        static final String TOPIC = "words-input1";
    static final String SERVERS="localhost:9092";
//    static final String SERVERS="192.168.56.1:29092";
//    static final String SERVERS="172.24.23    5.1:9092";

    // static final String SERVERS="192.168.0.115:9092,lo   calhost:9093,localhost:9094";

    public static void main(String[] args) throws IOException {

        Properties properties = new Properties();
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        System.out.println(properties);

        KafkaProducer<String, byte[]> producer = new KafkaProducer(properties);

        try {

            ProducerRecord<String, byte[]> producerRecord = null;
            Random random = new Random();
            for (int i = 0; i < 5; i++) {
                for (Employee e : Employees.getEmployees()) {
                    e.setId(random.nextInt(1000));
                    ObjectMapper Obj = new ObjectMapper();
                    String jsonStr = Obj.writeValueAsString(e);
                    GenericRecord record = buildRecord();
                    record.put("value",jsonStr);

                    DatumWriter<GenericRecord> writer = new SpecificDatumWriter<>(Message.getClassSchema());
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
                    writer.write(record, encoder);
                    encoder.flush();
                    out.close();

                    producerRecord = new ProducerRecord(TOPIC, out.toByteArray());
                    producer.send(producerRecord);
                    System.out.println(producerRecord.toString());

                }
            }
        } catch (Exception ex) {
            System.out.println(ex);
        } finally {
            producer.close();
        }
    }

    public static GenericRecord buildRecord() throws IOException {
        String schemaPath = "message.avsc";
//        System.out.println();
        Stream<String> schemaString = Files.lines(Paths.get("src","main","resources","message.avsc"));
        String result = schemaString.collect(Collectors.joining(" "));
        System.out.println("result =>" + result);
        Schema schema = new Schema.Parser().parse(result);
        GenericData.Record record = new GenericData.Record(schema);
        return record;
    }
}
