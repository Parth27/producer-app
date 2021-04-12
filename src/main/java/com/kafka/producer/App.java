package com.kafka.producer;

import com.kafka.config.KafkaConfig;

import java.io.IOException;
import java.net.URISyntaxException;

public class App 
{
    public static void main( String[] args ) throws IOException, URISyntaxException {
        MessageProducer producer = new MessageProducer();
        System.out.println( "Hello World!" );
        producer.run();
    }
}
