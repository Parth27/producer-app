package com.kafka.producer;

import java.io.IOException;
import java.net.URISyntaxException;

public class App 
{
    public static void main( String[] args ) throws IOException, URISyntaxException {
        MessageProducer producer = new MessageProducer("Messages", 50);
        System.out.println( "Hello World!" );
        producer.run();
    }
}
