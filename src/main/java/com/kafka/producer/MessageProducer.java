package com.kafka.producer;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;
import java.util.Random;

import com.kafka.producer.config.ProducerConfig;
import com.kafka.serializer.ArrayListSerializer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class MessageProducer extends Thread {
    List<String> messages;
    Random random;
    Producer<Integer, String> producer;
    String servers;
    int batchSize;
    boolean running;

    public MessageProducer(String servers, int batchSize) throws IOException, URISyntaxException {
        this.servers = servers;
        messages = new ArrayList<>();
        random = new Random(42); // Set seed
        String fileName = "data/Dataset_Emails.xlsx";
        FileInputStream fis = new FileInputStream(getFileFromResource(fileName));
        loadMessages(fis);
        this.batchSize = batchSize;
        running = false;
    }

    @Override
    public void run() {
        System.out.println("Servers: " + servers);
        producer = getProducer();
        int id = 0;
        List<String> batch = new ArrayList<>();
        System.out.println("Started producer");
        running = true;
        while (!isInterrupted()) {
            // for (int i = 0; i < batchSize; i++) {
            //     batch.add(messages.get(random.nextInt(messages.size())));
            // }
            String message = messages.get(random.nextInt(messages.size()));
            ProducerRecord<Integer, String> record = new ProducerRecord<>(ProducerConfig.TOPIC, id, message);
            producer.send(record);
            id++;
            // batch.clear();
            // try {
            //     Thread.sleep(ProducerConfig.FREQUENCY * 1000);
            // } catch (InterruptedException e) {
            //     Thread.currentThread().interrupt();
            // }
        }
        running = false;
        producer.close();
    }

    private Producer<Integer, String> getProducer() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("acks", "all");
        properties.put("retries", 0);
        properties.put("buffer.memory", 33554432);
        properties.setProperty("key.serializer", IntegerSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        return new KafkaProducer<>(properties);
    }

    private File getFileFromResource(String fileName) throws URISyntaxException {

        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("file not found! " + fileName);
        } else {
            return new File(resource.toURI());
        }
    }

    private void loadMessages(FileInputStream fis) throws IOException {
        XSSFWorkbook wb = new XSSFWorkbook(fis);
        XSSFSheet sheet = wb.getSheetAt(0); // creating a Sheet object to retrieve object
        Iterator<Row> itr = sheet.iterator(); // iterating over excel file
        Row row;
        row = itr.next();
        String sentence;
        while (itr.hasNext()) {
            try {
                sentence = row.getCell(2).getStringCellValue();
                messages.add(sentence);
            } catch (IllegalStateException e) {
                // do nothing
            } finally {
                row = itr.next();
            }
        }
        wb.close();
    }

    class ProducerStop extends Thread {
        @Override
        public void run() {
            try {
                System.out.println("Stoping producer...");
                running = false;
                producer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}