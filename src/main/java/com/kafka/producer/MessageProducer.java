package com.kafka.producer;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
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

import java.util.List;
import java.util.ArrayList;
import java.util.Random;
import java.util.Iterator;
import java.util.Properties;

import java.net.URI;
import java.net.URL;
import java.net.URISyntaxException;

import com.kafka.config.KafkaConfig;
import com.kafka.serializer.ArrayListSerializer;

public class MessageProducer {
    List<String> messages;
    Random random;
    Producer<Integer, List<String>> producer;

    public MessageProducer() {
        messages = new ArrayList<>();
        random = new Random(42); //Set seed
        producer = getProducer();
    }
    private Producer<Integer, List<String>> getProducer() {
		Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", KafkaConfig.SERVERS);
        properties.setProperty("acks","all");
        properties.put("retries", 0);
        properties.put("buffer.memory", 33554432);
        properties.setProperty("key.serializer", IntegerSerializer.class.getName());
        properties.setProperty("value.serializer", ArrayListSerializer.class.getName());
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
    private void loadMessages(FileInputStream fis) {
        try(XSSFWorkbook wb = new XSSFWorkbook(fis)) {
            XSSFSheet sheet = wb.getSheetAt(0);     //creating a Sheet object to retrieve object
            Iterator<Row> itr = sheet.iterator();    //iterating over excel file
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
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public void run() throws IOException, URISyntaxException {
        String fileName = "data/All_emails1.xlsx";
        FileInputStream fis = new FileInputStream(getFileFromResource(fileName));
        loadMessages(fis);
        for (int i=0; i < 20; i++) {
            System.out.println(messages.get(i));
        }
        int batchSize;
        int id = 0;
        List<String> batch = new ArrayList<>();
        while (id < 100000) {
            batchSize = (int)random.nextGaussian()*5 + KafkaConfig.MEAN_BATCHSIZE;
            for (int i=0; i < batchSize; i++) {
                batch.add(messages.get(random.nextInt(messages.size())));
            }
            ProducerRecord<Integer, List<String>> record = new ProducerRecord<>(KafkaConfig.TOPIC, id, batch);
            producer.send(record);
            id++;
            batch.clear();
        }
        producer.close();
    }
}