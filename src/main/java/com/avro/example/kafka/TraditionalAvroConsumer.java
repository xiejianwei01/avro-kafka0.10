package com.avro.example.kafka;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TraditionalAvroConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.236:6667");
        props.put("group.id", "dev3-yangyunhe-group001");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", AvroDeserializer.class.getName());
        KafkaConsumer<String, Stock> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList("dev3-yangyunhe-topic001"));

        try {
            while (true) {
                ConsumerRecords<String, Stock> records = consumer.poll(100);
                for (ConsumerRecord<String, Stock> record : records) {
                    Stock stock = record.value();
                    System.out.println(stock.toString());
                }
            }
        } finally {
            consumer.close();
        }
    }
}