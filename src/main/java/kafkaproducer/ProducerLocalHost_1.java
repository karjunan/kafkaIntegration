package kafkaproducer;

import com.google.gson.JsonObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ProducerLocalHost_1 {

    static final String TOPIC = "persons";

//        static final String TOPIC = "words-input1";
//    static final String SERVERS="pkc-4ygw7.ap-southeast-2.aws.confluent.cloud:9092";
    static final String SERVERS="localhost:9092";
//    static final String SERVERS="172.24.23    5.1:9092";

    // static final String SERVERS="192.168.0.115:9092,lo   calhost:9093,localhost:9094";

    public static void main(String[] args) throws IOException {

        Properties properties = new Properties();
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092,localhost:9093,localhost:9094");
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "http://localhost:8081");
//
        //   ssl.endpoint.identification.algorithm=https
        //   sasl.mechanism=PLAIN
        //   request.timeout.ms=20000
        //   bootstrap.servers=<CLUSTER_BOOTSTRAP_SERVER>
        //   retry.backoff.ms=500
        //   sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required username="<CLUSTER_API_KEY>" password="<CLUSTER_API_SECRET>";
        //   security.protocol=SASL_SSL
        //   basic.auth.credentials.source=USER_INFO
        //   schema.registry.basic.auth.user.info=<SR_API_KEY>:<SR_API_SECRET>
        //   schema.registry.url=https://<SR ENDPOINT>

        System.out.println(properties);

        KafkaProducer<String, byte[]> producer = new KafkaProducer(properties);

        String str = "Hey Dude";

        int val = 0;
        while(true) {
            JsonObject jsonObject = new JsonObject();
            jsonObject.addProperty("FIRSTNAME","krishna - " + val++);
            jsonObject.addProperty("LASTNAME","Arjunan");
            jsonObject.addProperty("BIRTHDAY","04-09-1909");
            producer.send(new ProducerRecord(TOPIC,jsonObject.toString()));
            producer.flush();
            System.out.println("Persisted " + producer);
//            try {
//                Thread.sleep(10000);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
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
