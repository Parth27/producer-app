package com.kafka.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.LongSerializer;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.IOException;

import java.util.Iterator;
import java.util.Properties;

import java.net.URI;
import java.net.URL;
import java.net.URISyntaxException;

public class MessageProducer {
    String topic;
    File f;
    FileInputStream fis;
    public MessageProducer(String topic) {
        this.topic = topic;
    }
    private Producer<Long, String> getProducer() {
		Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092");
        properties.setProperty("acks","all");
        properties.put("retries", 0);
        properties.put("buffer.memory", 33554432);
        properties.setProperty("key.serializer", LongSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
		return new KafkaProducer<>(properties);
	}
    private File getFileFromResource(String fileName) throws URISyntaxException{

        ClassLoader classLoader = getClass().getClassLoader();
        URL resource = classLoader.getResource(fileName);
        if (resource == null) {
            throw new IllegalArgumentException("file not found! " + fileName);
        } else {

            // failed if files have whitespaces or special characters
            //return new File(resource.getFile());

            return new File(resource.toURI());
        }

    }
    public void run() throws IOException, URISyntaxException {
        Producer<Long, String> producer = getProducer();
        String fileName = "data/All_emails1.xlsx";
        File file = getFileFromResource(fileName);
        fis = new FileInputStream(file);
        System.out.println(file);
        try(XSSFWorkbook wb = new XSSFWorkbook(fis)) {
            XSSFSheet sheet = wb.getSheetAt(0);     //creating a Sheet object to retrieve object 
            while (true) {
                break;
            }
        }
        producer.close();
    }
}