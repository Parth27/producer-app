package com.kafka.serializer;

import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Map;

public class ArrayListSerializer implements Serializer<ArrayList<String>> {
    @Override
    public void configure(Map<String, ?> config, boolean isKey) {
        // Empty function
    }

    @Override
    public byte[] serialize(String topic, ArrayList<String> arrayList) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        DataOutputStream out = new DataOutputStream(baos);
        for (String element : arrayList) {
            try {
                out.writeUTF(element);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return baos.toByteArray();
    }

    @Override
    public void close() {
        // Empty function
    }
}
