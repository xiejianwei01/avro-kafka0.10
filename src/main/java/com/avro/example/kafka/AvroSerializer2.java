package com.avro.example.kafka;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroSerializer2<T extends SpecificRecordBase> implements Serializer<T> {

    private static final Logger logger = LoggerFactory.getLogger(AvroSerializer.class);

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public byte[] serialize(String topic, T data) {
        DatumWriter<T> datumWriter = new SpecificDatumWriter<>(data.getSchema());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        try {
            BinaryEncoder binaryEncoder = EncoderFactory.get().binaryEncoder(out, null);
            datumWriter.write(data, binaryEncoder);
            binaryEncoder.flush();
        } catch (IOException e) {
            logger.error("avro serializer error.", e);
        }

        return out.toByteArray();
    }
}