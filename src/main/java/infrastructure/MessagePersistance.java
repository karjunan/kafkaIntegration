package infrastructure;

public interface MessagePersistance {

    /*
        Provide custom implementation for save method
    */
//    void save(Messages.ProducerRequest request,
//              Properties properties,
//              StreamObserver<Messages.OkResponse> responseObserver);
//
//    /*
//        There is a default implementation to persist the records in kafka.
//     */
//    default void saveDefault(Messages.ProducerRequest request,
//                             KafkaProducer<String, GenericRecord> producer) {
//        GenericRecord record = MessagePersistance.getAvroRecord(request);
//        Headers headers = MessagePersistance.getRecordHaders(request.getHeader());
//        for(String topic: request.getTopicList()) {
//            ProducerRecord<String,GenericRecord> producerRecord = new ProducerRecord<>
//                    (topic,request.getPartition(),request.getKey(),record,headers);
//            System.out.println(producerRecord);
//            producer.send(producerRecord);
//        }
//    }
//
//    static Headers getRecordHaders(Messages.Header protoHeader) {
//        Headers headers = new RecordHeaders();
//        protoHeader.getPairsMap().entrySet()
//                .forEach(k -> {
//                    Header header = new RecordHeader(k.getKey(),k.getValue().getBytes());
//                    headers.add(header);
//                });
//        return headers;
//    }
//
//    static GenericRecord getAvroRecord(Messages.ProducerRequest request) {
//        Schema.Parser parser = new Schema.Parser();
//        Schema schema = parser.parse(request.getAvroSchema());
//        GenericRecord avroRecord = new GenericData.Record(schema);
//        avroRecord.put("value",request.getValue());
//        return avroRecord;
//    }

}
