package com.avro.example.kafka;

import com.avro.example.User;
import jdk.nashorn.internal.objects.annotations.Setter;
import org.apache.avro.specific.SpecificRecordBase;

import org.apache.avro.specific.SpecificRecordBase;

import java.util.EnumSet;

public enum TopicEnum {
    USER("user-info-topic", new User());

    public final String topicName;
    public final SpecificRecordBase topicType;

    TopicEnum(String topicName, SpecificRecordBase topicType) {
        this.topicName = topicName;
        this.topicType = topicType;
    }

    public static TopicEnum matchFor(String topicName) {
        return EnumSet.allOf(TopicEnum.class).stream()
                .filter(topic -> topic.topicName.equals(topicName))
                .findFirst()
                .orElse(null);
    }
}