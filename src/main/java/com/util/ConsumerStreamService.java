package com.util;
//
//import com.grpc.server.proto.KafkaServiceGrpc;
//import com.grpc.server.proto.Messages;
//import io.grpc.Status;
//import io.grpc.stub.StreamObserver;
//import lombok.extern.log4j.Log4j;
//import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
//import org.springframework.cloud.stream.annotation.EnableBinding;
//import org.springframework.cloud.stream.annotation.Input;
//import org.springframework.cloud.stream.annotation.StreamListener;
//import org.springframework.messaging.SubscribableChannel;
//import org.springframework.stereotype.Service;
//
//import java.util.Queue;
//import java.util.concurrent.LinkedBlockingQueue;
//
//@Log4j
//@Service
//public class ConsumerStreamService extends KafkaServiceGrpc.KafkaServiceImplBase {
//
//    private static Queue<String> queue = new LinkedBlockingQueue<>();
//
//    @Override
//    public void getAll(Messages.GetAllMessages request, StreamObserver<Messages.Response> responseObserver) {
//        try{
//            queue.forEach(v -> {
//                System.out.println(queue);
//                Messages.Event event = Messages.Event.newBuilder().setValue(v).build();
//                responseObserver.onNext(Messages.Response.newBuilder().setEvent(event).build());
//                queue.poll();
//
//            });
//        } catch (Exception ex) {
//            responseObserver.onError(Status.CANCELLED
//                    .withDescription("Something went wrong while processing the request. Please restart !!")
//                    .asRuntimeException());
//            return;
//        }
//
//    }
//
//    @ConditionalOnProperty(name = "consumerBinding", havingValue = "true")
//    @EnableBinding(ConsumerReader.ConsumerChannel.class)
//    public static class ConsumerReader {
//
//
//        @StreamListener(value = ConsumerChannel.INPUT)
//        public void handle(String str) {
//            System.out.println(" Receiving Message from topic t1 => " + str);
//            queue.add(str);
//        }
//
//        interface ConsumerChannel {
//            String INPUT = "t1";
//
//            @Input(value = INPUT)
//            SubscribableChannel input();
//        }
//    }

//}

//        try {
//            while(true){
//                synchronized (queue) {
//                    queue.forEach(v -> {
//                        System.out.println(queue);
//                        MessagesConsumer.Event event = MessagesConsumer.Event.newBuilder().setValue(v).build();
//                        responseObserver.onNext(MessagesConsumer.Response.newBuilder().setEvent(event).build());
//                        queue.poll();
//                    });
//                }
//            }
//
//        } catch (Exception ex) {
//            responseObserver.onError(Status.CANCELLED
//                    .withDescription("Something went wrong while reading the meassage")
//                    .asRuntimeException());
//            return;
//        }
