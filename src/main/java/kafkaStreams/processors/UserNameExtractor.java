package kafkaStreams.processors;

import org.apache.kafka.streams.processor.AbstractProcessor;

public class UserNameExtractor extends AbstractProcessor<String,String> {

    @Override
    public void process(String key, String value) {

        System.out.println( key + " -  " + value.toUpperCase() );
        context().commit();

    }

}
