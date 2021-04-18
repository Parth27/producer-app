package com.kafka.producer;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URISyntaxException;

import com.kafka.config.KafkaConfig;

public class App {
    ServerSocket server;
    private volatile boolean running = true;
    Thread producer;
    String servers;

    public App() throws IOException, URISyntaxException {
        server = new ServerSocket(KafkaConfig.PORT);
        producer = new MessageProducer("");
        servers = "";
    }

    public void run() throws IOException, URISyntaxException {
        Socket socket;
        while (running) {
            socket = server.accept();
            DataInputStream dis = new DataInputStream(socket.getInputStream());
            if (!servers.equals("")) {
                producer.interrupt();
            }
            servers = dis.readUTF();
            producer = new MessageProducer(servers);
            producer.start();
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        App producerApp = new App();
        System.out.println("Started Kafka Producer...");
        producerApp.run();
    }
}
