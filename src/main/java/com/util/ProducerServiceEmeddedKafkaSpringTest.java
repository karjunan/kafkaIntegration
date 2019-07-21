//package com.util;
//
//import com.grpc.server.proto.KafkaServiceGrpc;
//import com.grpc.server.proto.Messages;
//import com.grpc.server.util.UtilHelper;
//import com.grpc.server.util.Utils;
//import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
//import io.grpc.testing.GrpcCleanupRule;
//import org.apache.avro.generic.GenericRecord;
//import org.apache.kafka.clients.consumer.Consumer;
//import org.apache.kafka.clients.consumer.ConsumerConfig;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//import org.apache.kafka.clients.consumer.ConsumerRecords;
//import org.apache.kafka.clients.producer.ProducerConfig;
//import org.junit.After;
//import org.junit.Before;
//import org.junit.Ignore;
//import org.junit.Rule;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
//import org.springframework.kafka.core.DefaultKafkaProducerFactory;
//import org.springframework.kafka.core.KafkaTemplate;
//import org.springframework.kafka.core.ProducerFactory;
//import org.springframework.kafka.support.SendResult;
//import org.springframework.kafka.test.utils.KafkaTestUtils;
//import org.springframework.test.context.junit4.SpringRunner;
//import org.springframework.util.concurrent.ListenableFuture;
//
//import java.time.Duration;
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//import java.util.concurrent.BlockingQueue;
//import java.util.concurrent.ExecutionException;
//import java.util.concurrent.LinkedBlockingQueue;
//
//@RunWith(SpringRunner.class)
////@ContextConfiguration(classes = {GrpcServer.class,ProducerService.class,KafkaProducerConfig.class,
////        KafkaProducerProperties.class,HeaderServerInterceptor.class,
////        GrpcApplication.class})
////@EnableKafka
////@EmbeddedKafka(partitions = 1, controlledShutdown = false,
////        brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
//public class ProducerServiceEmeddedKafkaSpringTest {
////
////    @Value( "${grpc.port}" )
////    private String port;
//
//    private static final String TOPIC_NAME = "topic-1";
//
////    @Autowired
////    GrpcServer grpcKafkaServer;
////
////    @Autowired
////    private ProducerService producerService;
////
////    @Autowired
////    private  KafkaProducerConfig kafkaProducerConfig;
////
////    @Autowired
////    HeaderServerInterceptor headerServerInterceptor;
//
////    @Autowired
////    EmbeddedKafkaBroker embeddedKafkaBroker;
//
//
//
////    @ClassRule
////    public static KafkaEmbedded embeddedKafka = new KafkaEmbedded(1, true, "TOPIC_NAME");
//
//    @Rule
//    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
//    private KafkaServiceGrpc.KafkaServiceBlockingStub kafkaServiceBlockingStub;
//
//    KafkaServiceGrpc.KafkaServiceBlockingStub blockingStub = null;
//
//    Messages.ProducerRequest producerRequest = null;
//    BlockingQueue<ConsumerRecord<String, GenericRecord>> queue = null;
//    DefaultKafkaConsumerFactory<String, GenericRecord> cf = null;
//
//    Consumer consumer = null;
//
//    @Ignore
//    @Before
//    public void setup() throws Exception {
//
//        // Generate a unique in-process server name.
////        String serverName = InProcessServerBuilder.generateName();
//
//        Map<String, Object> consumerProps =  KafkaTestUtils.consumerProps("localhost:9092","group1","false");
//
////                KafkaTestUtils.consumerProps("testT","false",embeddedKafkaBroker);
//
//        consumerProps.put(
//                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
//                "org.apache.kafka.common.serialization.StringSerializer");
//        consumerProps.put(
//                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//                "io.confluent.kafka.serializers.KafkaAvroSerializer");
////        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
//
//        cf = new DefaultKafkaConsumerFactory(consumerProps);
//        consumer = cf.createConsumer();
//
////        KafkaMessageListenerContainer<String,GenericRecord> container = new KafkaMessageListenerContainer(cf,);
//
//
//
//
//        // Create a server, add service, start, and register for automatic graceful shutdown.
////        grpcCleanup.register(InProcessServerBuilder
////                .forName(serverName).directExecutor().addService(producerService).build().start());
////
////        blockingStub = KafkaServiceGrpc.newBlockingStub(
////                // Create a client channel and register for automatic graceful shutdown.
////                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
////
////
////
//        Messages.Header header = Messages.Header.newBuilder()
//                .putPairs("corelationId","1234")
//                .putPairs("transcationId","5678")
//                .putPairs("avroSchema",UtilHelper.getAvroData())
//                .build();
//
//
//
//        List<String> list = new ArrayList<>();
//        list.add("topic-1");
//
//        producerRequest = Messages.ProducerRequest.newBuilder()
//                .addTopic("topic-1")
////                .addTopic("topic-2")
////                .setAvroSchema(UtilHelper.getAvroData())
//                .setValue("Finally its working")
//                .setHeader(header)
//                .build();
//
//    }
//
//    @Ignore
//    @After
//    public void tearDown() throws Exception {
////        testServer.stop();
//    }
//
//
////    @Ignore
//    @Test
//    public void save_message_successfully() throws InterruptedException, ExecutionException {
//
//        Map<String, Object> senderProps = KafkaTestUtils.senderProps("localhost:9092");
//        senderProps.put(
//                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
//                "org.apache.kafka.common.serialization.StringSerializer");
//        senderProps.put(
//                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
//                "io.confluent.kafka.serializers.KafkaAvroSerializer");
//        senderProps.put(
//                AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG,
//                "http://localhost:8081");
//        ProducerFactory<String, GenericRecord> pf =
//                new DefaultKafkaProducerFactory<>(senderProps);
//        KafkaTemplate<String, GenericRecord> template = new KafkaTemplate<>(pf);
//        template.setDefaultTopic(TOPIC_NAME);
//
//        ListenableFuture<SendResult<String,GenericRecord>> future = template.send(TOPIC_NAME,Utils.getAvroRecord(producerRequest));
//        future.get();
//
//
//        List<String> list = new ArrayList<>();
//        list.add(TOPIC_NAME);
//
//        consumer.subscribe(list);
//        queue = new LinkedBlockingQueue<>();
//            ConsumerRecords<String,GenericRecord> records = consumer.poll(Duration.ofMillis(100000));
//            for(ConsumerRecord<String,GenericRecord> rec : records) {
//                System.out.println("Value received is => " + rec.value());
//                queue.add(rec);
//            }
//
//        System.out.println(queue);
//
//    }
//
//
//}
