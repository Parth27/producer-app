package com.kafka.producer.config;

public class ProducerConfig {
    public static final String SERVERS = "3.137.149.32:9092,3.129.17.73:9092,3.133.148.122:9092";
    public static final String TOPIC = "Messages";
    public static final int MEAN_BATCHSIZE = 10;
    public static final int PORT = 4567;
    public static final long FREQUENCY = 15L;
}
