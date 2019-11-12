package com.avro.example.kafka;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

import com.avro.example.User;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class TraditionalAvroConsumer {

    public static void main(String[] args) {

        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.236:6667");
        props.put("group.id", "dev3-yangyunhe-group001");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", AvroDeserializer2.class.getName());
        KafkaConsumer<String, SpecificRecordBase> consumer = new KafkaConsumer<>(props);
       List<String> list = new ArrayList<>();
        for (TopicEnum value : TopicEnum.values()) {
            list.add(value.topicName);
            System.out.println(value.topicName);
        }
        consumer.subscribe(list);
        try {
            while (true) {
                ConsumerRecords<String, SpecificRecordBase> records = consumer.poll(100);
                for (ConsumerRecord<String, SpecificRecordBase> record : records) {
                    if(TopicEnum.STOCK.topicName.equals(record.topic())){
                        Stock stock = (Stock)record.value();
                        System.out.println(stock.toString());
                    }
                    if(TopicEnum.USER.topicName.equals(record.topic())){
                        User stock = (User)record.value();
                        System.out.println(stock.toString());
                    }
                }
            }
        } finally {
            consumer.close();
        }
    }
}