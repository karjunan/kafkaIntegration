package confluent;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.http.HttpEntity;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.Future;

public class ConfluentProducerAvro {

    static final String TOPIC = "t2";
    static final String SERVERS="pkc-4ygw7.ap-southeast-2.aws.confluent.cloud:9092";

    public static void main(String[] args) throws IOException, InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVERS);
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, io.confluent.kafka.serializers.KafkaAvroSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
//        properties.put("value.serializer","io.confluent.kafka.serializers.KafkaAvroSerializer");
        properties.put("ssl.endpoint.identification.algorithm","https");
        properties.put("sasl.mechanism","PLAIN");
        properties.put("request.timeout.ms","1000");
        properties.put("sasl.jaas.config","org.apache.kafka.common.security.plain.PlainLoginModule required username=\"HR7BMQFCH3FCA32I\" password=\"AFI+3XWz0f+JIF983A05Qy+T8G7i0GDyqLO//WDUPIR0pEYyrWK8u6w4gGgiuC1S\";");
        properties.put("security.protocol","SASL_SSL");
        properties.put("basic.auth.credentials.source","USER_INFO");
        properties.put("schema.registry.url", "https://psrc-l6oz3.us-east-2.aws.confluent.cloud");
        properties.put("schema.registry.basic.auth.user.info" ,"PBDH6NJ4XFQRTJPS:MQFEt01i9ECjMMgsVAnSdHvXrRQgU+sTBNLmmiUviVxtHAadM8uLOB9VboVfLUau");


//        KafkaProducer<String, GenericRecord> producer = new KafkaProducer(properties);
        System.out.println(properties);


        CloseableHttpClient httpClient = HttpClients.createDefault();
        String schema = "";
        try {
            CredentialsProvider credsProvider = new BasicCredentialsProvider();
            credsProvider.setCredentials(

                    new AuthScope("https://psrc-l6oz3.us-east-2.aws.confluent.cloud/subjects/T2-value/versions/1/schema",80),
                    new UsernamePasswordCredentials("PBDH6NJ4XFQRTJPS", "MQFEt01i9ECjMMgsVAnSdHvXrRQgU+sTBNLmmiUviVxtHAadM8uLOB9VboVfLUau"));

//            CredentialsProvider creds = new UsernamePasswordCredentials("PBDH6NJ4XFQRTJPS", "MQFEt01i9ECjMMgsVAnSdHvXrRQgU+sTBNLmmiUviVxtHAadM8uLOB9VboVfLUau");
//            CloseableHttpClient httpClient = HttpClients.custom().setDefaultCredentialsProvider(credsProvider).build();
//            HttpGet request = new HttpGet("https://psrc-l6oz3.us-east-2.aws.confluent.cloud/subjects/T2-value/versions/1/schema");

            HttpGet request = new HttpGet("https://PBDH6NJ4XFQRTJPS:MQFEt01i9ECjMMgsVAnSdHvXrRQgU+sTBNLmmiUviVxtHAadM8uLOB9VboVfLUau@psrc-l6oz3.us-east-2.aws.confluent.cloud/subjects/t3-value/versions/1/schema");
//            HttpGet request = new HttpGet("https://PBDH6NJ4XFQRTJPS:MQFEt01i9ECjMMgsVAnSdHvXrRQgU+sTBNLmmiUviVxtHAadM8uLOB9VboVfLUau@psrc-l6oz3.us-east-2.aws.confluent.cloud/subjects/T2-value/versions/1/schema");
            CloseableHttpResponse response = httpClient.execute(request);
            HttpEntity entity = response.getEntity();
            schema = EntityUtils.toString(entity);
            System.out.println("Output is => " + schema);
        } catch (Exception ex) {

        }
        try {

            Schema avroValueSchema = new Schema.Parser().parse(schema);

            final KafkaProducer<String, Object> producer = new KafkaProducer<>(properties);
            for (int i = 0; i < 5; i++) {
                GenericRecordBuilder builder = new GenericRecordBuilder(avroValueSchema);
                builder.set("id","krishna");
//                builder.set("amount", 100d);
                GenericData.Record record = builder.build();
                Future<RecordMetadata> future = producer.send(new ProducerRecord<>("t3",record));
                System.out.println("Message Sent " + future.get().offset());
            }

        } catch (Exception ex ) {
            ex.printStackTrace();
            System.out.println(ex);

        }

    }
}
