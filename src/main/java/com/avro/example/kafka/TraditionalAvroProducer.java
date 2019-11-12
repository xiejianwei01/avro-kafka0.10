package com.avro.example.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class TraditionalAvroProducer {
    
    public static void main(String[] args) throws Exception {
        
        Stock[] stocks = new Stock[100];
        for(int i = 0; i < 100; i++) {
            stocks[i] = new Stock();
            stocks[i].setStockCode(String.valueOf(i));
            stocks[i].setStockName("stock" + i);
            stocks[i].setTradeTime(System.currentTimeMillis());
            stocks[i].setPreClosePrice(100.0F);
            stocks[i].setOpenPrice(88.8F);
            stocks[i].setCurrentPrice(120.5F);
            stocks[i].setHighPrice(300.0F);
            stocks[i].setLowPrice(12.4F);
        }
        
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.1.236:6667");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", AvroSerializer2.class.getName());

        Producer<String, Stock> producer = new KafkaProducer<>(props);
        
        for(Stock stock : stocks) {
            ProducerRecord<String, Stock> record = new ProducerRecord<>("dev3-yangyunhe-topic001", stock);
            RecordMetadata metadata = producer.send(record).get();
            StringBuilder sb = new StringBuilder();
            sb.append("stock: ").append(stock.toString()).append(" has been sent successfully!").append("\n")
                .append("send to partition ").append(metadata.partition())
                .append(", offset = ").append(metadata.offset());
            System.out.println(sb.toString());
            Thread.sleep(100);
        }
        
        producer.close();
    }
}
