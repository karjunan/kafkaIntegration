package kafkaconsumergroups;

import com.grpc.server.avro.Message;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;

import java.io.IOException;

public class ConsumerDeserialization {

//
//    @Bean
//    @StreamMessageConverter
//    public MessageConverter customMessageConverter() {
//        return new MessageConverter();
//    }
//    public class MessageConverter extends AbstractMessageConverter {
//
//        public MessageConverter() {
//            super(new MimeType("application","*+avro"));
//        }
//
//        @Override
//        protected boolean supports(Class<?> aClass) {
//            return (GenericRecord.class.equals(aClass));
//        }
//
//        @Override
//        protected Object convertFromInternal(Message<?> message, Class<?> targetClass, Object conversionHint) {
//            Object payload = message.getPayload();
//            System.out.println("Current payload " + payload.toString());
//            try {
//                Decoder decoder = DecoderFactory.get().binaryDecoder((byte[]) message.getPayload(), null);
//                DatumReader<Message> reader = new SpecificDatumReader<>(com.grpc.server.avro.Message.getClassSchema());
//                com.grpc.server.avro.Message msg = reader.read(null , decoder);
//                return msg;
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//            return null;
//        }
//    }
}
