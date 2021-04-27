package com.kafka.producer;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URISyntaxException;
import java.util.Scanner;

import com.kafka.producer.config.ProducerConfig;

public class App {
    private volatile boolean running = true;
    MessageProducer producer;
    String servers;

    public App() throws IOException, URISyntaxException {
        producer = new MessageProducer(ProducerConfig.SERVERS, ProducerConfig.MEAN_BATCHSIZE);
        servers = "";
    }

    public void run() throws IOException, URISyntaxException {
        try (ServerSocket server = new ServerSocket(ProducerConfig.PORT)) {
            Socket socket;
            while (running) {
                socket = server.accept();
                DataInputStream dis = new DataInputStream(socket.getInputStream());
                if (producer.running) {
                    producer.interrupt();
                    try {
                        producer.join();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
                servers = dis.readUTF();
                producer = new MessageProducer(servers, ProducerConfig.MEAN_BATCHSIZE);
                Runtime.getRuntime().addShutdownHook(producer.new ProducerStop());
                Thread batchModifier = new BatchModifier();
                batchModifier.start();
                producer.start();
            }
        }
    }

    public void runDemo() {
        while (running) {
            Runtime.getRuntime().addShutdownHook(producer.new ProducerStop());
            producer.start();
        }
    }

    class BatchModifier extends Thread {
        Scanner input;

        public BatchModifier() {
            System.out.println("Startup Batch Size = "+ProducerConfig.MEAN_BATCHSIZE);
            input = new Scanner(System.in);
        }

        @Override
        public void run() {
            while (running) {
                int newBatchSize = input.nextInt();
                producer.interrupt();
                try {
                    producer.join();
                    producer = new MessageProducer(servers, newBatchSize);
                    producer.start();
                } catch (IOException | URISyntaxException | InterruptedException e) {
                    e.printStackTrace();
                    Thread.currentThread().interrupt();
                }
                System.out.println("Modify Batch Size:\t");
            }
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        App producerApp = new App();
        producerApp.run();
        // producerApp.runDemo();
    }
}
