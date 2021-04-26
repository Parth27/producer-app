package com.kafka.producer.config;

public class ProducerConfig {
    public static final String SERVERS = "localhost:9092";
    public static final String TOPIC = "Messages";
    public static final int MEAN_BATCHSIZE = 10;
    public static final int PORT = 4567;
    public static final long FREQUENCY = 15L;
}
