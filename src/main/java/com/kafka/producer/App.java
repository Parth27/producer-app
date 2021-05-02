package com.kafka.producer;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Scanner;

import com.kafka.producer.config.ProducerConfig;

public class App {
    private volatile boolean running = true;
    MessageProducer producer;

    public App() throws IOException, URISyntaxException {
        producer = new MessageProducer(ProducerConfig.SERVERS, ProducerConfig.MESSAGE_RATE);
    }

    public void run() throws IOException, URISyntaxException {
        while (running) {
            Runtime.getRuntime().addShutdownHook(new App().new ProducerStop());
            producer.start();
        }
    }

    class ProducerStop extends Thread {
        @Override
        public void run() {
            try {
                System.out.println("Stoping producer...");
                running = false;
                producer.interrupt();
                System.out.println("Enter new server list (press ^C to exit): ");
                Scanner in = new Scanner(System.in);
                String serverList = in.nextLine();
                producer = new MessageProducer(serverList, ProducerConfig.MESSAGE_RATE);
                in.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public static void main(String[] args) throws IOException, URISyntaxException {
        App producerApp = new App();
        producerApp.run();
    }
}
