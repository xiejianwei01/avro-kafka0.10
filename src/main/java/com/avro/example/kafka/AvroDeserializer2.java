package com.avro.example.kafka;

import com.sun.org.apache.xalan.internal.xsltc.compiler.sym;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DatumReader;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Map;

public class AvroDeserializer2<T extends SpecificRecordBase> implements Deserializer<T> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {}

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null){
            return null;
        }
        try {
//            或者主题类型
            TopicEnum topicEnum = TopicEnum.matchFor(topic);
            if (topicEnum == null){
                return null;
            }

            SpecificRecordBase record = topicEnum.topicType;
            DatumReader<T> datumReader = new SpecificDatumReader<>(record.getSchema());
            ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(data);
            BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(byteArrayInputStream,null);
            return  datumReader.read(null,decoder);

        }catch (IOException e){
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void close() {}
}
