//package com.util;
//
//import com.grpc.server.GrpcApplication;
//import com.grpc.server.config.KafkaProducerConfig;
//import com.grpc.server.config.KafkaProducerProperties;
//import com.grpc.server.interceptor.HeaderServerInterceptor;
//import com.grpc.server.proto.KafkaServiceGrpc;
//import com.grpc.server.proto.Messages;
//import com.grpc.server.server.GrpcServer;
//import com.grpc.server.service.ProducerService;
//import com.grpc.server.util.KafkaGrpcTestServer;
//import com.grpc.server.util.UtilHelper;
//import com.grpc.server.util.ZookeeperGrpcTestServer;
//import com.salesforce.kafka.test.KafkaTestServer;
//import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
//import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
//import io.grpc.inprocess.InProcessChannelBuilder;
//import io.grpc.inprocess.InProcessServerBuilder;
//import io.grpc.testing.GrpcCleanupRule;
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//import org.junit.Ignore;
//import org.junit.Rule;
//import org.junit.Test;
//import org.junit.runner.RunWith;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.beans.factory.annotation.Value;
//import org.springframework.test.context.ContextConfiguration;
//import org.springframework.test.context.junit4.SpringRunner;
//
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.util.ArrayList;
//import java.util.Comparator;
//import java.util.List;
//import java.util.Properties;
//
//@RunWith(SpringRunner.class)
//@ContextConfiguration(classes = {GrpcServer.class,ProducerService.class,KafkaProducerConfig.class,
//        KafkaProducerProperties.class,HeaderServerInterceptor.class,
//        GrpcApplication.class})
////@EnableKafka
////@EmbeddedKafka(partitions = 1, controlledShutdown = false,
////        brokerProperties = {"listeners=PLAINTEXT://localhost:9092", "port=9092"})
//public class ProducerServiceEmeddedIntegrationTest {
//
//    @Value( "${grpc.port}" )
//    private String port;
//
//    @Autowired
//    GrpcServer grpcKafkaServer;
//
//    @Autowired
//    private ProducerService producerService;
//
//    @Autowired
//    private  KafkaProducerConfig kafkaProducerConfig;
//
//    @Autowired
//    HeaderServerInterceptor headerServerInterceptor;
//
//    KafkaTestServer testServer = null;
//
//    List<String> serverList = new ArrayList<>();
//    List<String> schemaRegistryList = new ArrayList<>();
//
//    Properties properties;
//
//   SchemaRegistryClient schemaRegistryClient = new MockSchemaRegistryClient();
//
//
//    @Rule
//    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
//    private KafkaServiceGrpc.KafkaServiceBlockingStub kafkaServiceBlockingStub;
//
//    KafkaServiceGrpc.KafkaServiceBlockingStub blockingStub = null;
//    Messages.ProducerRequest producerRequest = null;
//
//    @Ignore
//    @Before
//    public void setup() throws Exception {
//
//        Path pathToBeDeleted = Paths.get("\\tmp");
//        Files.walk(pathToBeDeleted)
//                .sorted(Comparator.reverseOrder())
//                .map(Path::toFile)
//                .forEach(v -> {
//                    System.out.println(v);
////                    v.deleteOnExit();
//                    v.delete();
//                    });
//
//        ZookeeperGrpcTestServer zookeeperGrpcTestServer = new ZookeeperGrpcTestServer();
//        properties = new Properties();
//        testServer = new KafkaGrpcTestServer(properties,zookeeperGrpcTestServer);
//        testServer.start();
//        // Generate a unique in-process server name.
//        String serverName = InProcessServerBuilder.generateName();
//
//        // Create a server, add service, start, and register for automatic graceful shutdown.
//        grpcCleanup.register(InProcessServerBuilder
//                .forName(serverName).directExecutor().addService(producerService).build().start());
//
//        blockingStub = com.grpc.server.proto.KafkaServiceGrpc.newBlockingStub(
//                // Create a client channel and register for automatic graceful shutdown.
//                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
//
//
//
//        Messages.Header header = Messages.Header.newBuilder()
//                .putPairs("corelationId","1234")
//                .putPairs("transcationId","5678")
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
//                .setAvroSchema(UtilHelper.getAvroData())
//                .setValue("Finally its working")
//                .setHeader(header)
//                .build();
//
//    }
//
//    @Ignore
//    @After
//    public void tearDown() throws Exception {
//        testServer.stop();
//    }
//
//
//    @Ignore
//    @Test
//    public void save_message_successfully()  {
//        Messages.OkResponse reply =  blockingStub.save(producerRequest);
//        Assert.assertEquals(true,reply.getIsOk());
//
//    }
//
//
//}
