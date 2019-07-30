package kafkaStreams;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.Decoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.commons.lang.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import javax.xml.bind.DatatypeConverter;
import java.util.Arrays;
import java.util.Map;

public class AvroDeserializer implements Deserializer{



    @Override
    public void configure(Map configs, boolean isKey) {

    }

    @Override
    public GenericRecord deserialize(String topic, byte[] data) {
//        try {
//            T result = null;
//
//            if (data != null) {
//                System.out.println("data='{}'" + DatatypeConverter.printHexBinary(data));
//                DatumReader<GenericRecord> datumReader =
//                        new SpecificDatumReader<>(.newInstance().getSchema());
//                Decoder decoder = DecoderFactory.get().binaryDecoder(data, null);
//
//                result = (T) datumReader.read(null, decoder);
//                System.out.println("deserialized data='{}'" + result);
//            }
//            return result;
//        } catch (Exception ex) {
//            throw new SerializationException(
//                    "Can't deserialize data '" + Arrays
//                            .toString(data) + "' from topic '" + topic + "'", ex);
//        }
        return null;
    }

    @Override
    public void close() {

    }


}
