package kafkaconsumergroups;

//import com.grpc.server.proto.KafkaConsumerServiceGrpc;
//import lombok.extern.log4j.Log4j;
//import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
//import org.springframework.stereotype.Service;
//
//@Log4j
//@Service
//@ConditionalOnProperty(name = "consumerBinding", havingValue = "true")
//public class ConsumerStreamService extends KafkaConsumerServiceGrpc.KafkaConsumerServiceImplBase {
//
//
//    private Queue<String> queue = new LinkedBlockingQueue<>();
//
//    private StreamObserver<MessagesConsumer.Response> responseObserver;
//
//    @Autowired
//    public ConsumerStreamService(KafkaConsumerProperties kafkaConsumerProperties,
//                                 KafkaStreamsConfig kafkaStreamsConfig) {
//        StreamsBuilder nameAgeBuilder = new StreamsBuilder();
//        log.info("Starting Consumer stream service ...");
//        KStream<String, String> name = nameAgeBuilder.stream(kafkaConsumerProperties.getTopic(), Consumed.with(Serdes.String(), Serdes.String()));
//        name.foreach((k,v) -> {
//            System.out.println(" Received Messaage => " + v);
//            MessagesConsumer.Event event = MessagesConsumer.Event.newBuilder().setValue(v).build();
//            if(this != null && this.responseObserver != null) {
//                this.responseObserver.onNext(MessagesConsumer.Response.newBuilder().setEvent(event).build());
//            }
//
//        });
//        KafkaStreams kafkaStreams = new KafkaStreams(nameAgeBuilder.build(),kafkaStreamsConfig.config());
//        kafkaStreams.start();
//    }
//
//    @Override
//    public void getAll(MessagesConsumer.GetAllMessages request,
//                       StreamObserver<MessagesConsumer.Response> responseObserver) {
//
//        try {
//            this.responseObserver = responseObserver;
//        } catch (Exception ex) {
//            responseObserver.onError(Status.INTERNAL.withDescription(ex.getMessage())
//                    .augmentDescription(ex.getCause().getMessage())
//                    .withCause(ex.getCause())
//                    .asRuntimeException());
//            return;
//        }
//
//    }


//}
