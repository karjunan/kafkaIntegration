package kafkaproducer;

public class ProducerServiceRemoteKafkaTest {

//    /**
//     * This rule manages automatic graceful shutdown for the registered servers and channels at the
//     * end of test.
//     */
//    @Rule
//    public final GrpcCleanupRule grpcCleanup = new GrpcCleanupRule();
//
//    @Rule
//    public ExpectedException thrown = ExpectedException.none();
//
//
//    static final String schemaRegistry = "http://localhost:8081";
//    static final String server = "http://localhost:9092";
//
//    List<String> serverList = new ArrayList<>();
//    List<String> schemaRegistryList = new ArrayList<>();
//
//    Properties properties;
//
//    @Before
//    public void setup() throws Exception {
//
//        serverList.add(server);
//        schemaRegistryList.add(schemaRegistry);
//
//        properties = new Properties();
//
//        final String BOOTSTRAP_SERVERS = serverList.stream().collect(Collectors.joining(","));
//        final String SCHEMA_REGISTRIES = schemaRegistryList.stream().collect(Collectors.joining(","));
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
//
//        properties = new Properties();
//        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
//        properties.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, SCHEMA_REGISTRIES);
//
//    }
//
//    @Test
//    public void save_message_successfully() throws Exception {
//
//        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
//        // Generate a unique in-process server name.
//        String serverName = InProcessServerBuilder.generateName();
//
//        // Create a server, add service, start, and register for automatic graceful shutdown.
//        grpcCleanup.register(InProcessServerBuilder
//                .forName(serverName).directExecutor().addService(new ProducerService()).build().start());
//
//        com.grpc.server.proto.KafkaServiceGrpc.KafkaServiceBlockingStub blockingStub = com.grpc.server.proto.KafkaServiceGrpc.newBlockingStub(
//                // Create a client channel and register for automatic graceful shutdown.
//                grpcCleanup.register(InProcessChannelBuilder.forName(serverName).directExecutor().build()));
//
//
//        Messages.Header header = Messages.Header.newBuilder()
//                .putPairs("corelationId", "1234")
//                .putPairs("transcationId", "5678")
//                .build();
//
//        Messages.ProducerRequest producerRequest = Messages.ProducerRequest.newBuilder()
//                .addTopic("t1")
//                .addTopic("t2")
//                .setAvroSchema(Utils.getAvroData())
//                .setValue("Finally its working")
//                .setHeader(header)
//                .build();
//
//        Messages.OkResponse reply = blockingStub.save(producerRequest);
//
//        Assert.assertEquals(true, reply.getIsOk());
//
//    }


}
