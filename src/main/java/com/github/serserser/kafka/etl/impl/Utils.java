package com.github.serserser.kafka.etl.impl;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class Utils {

    public static Properties createKafkaProperties(Class<?> valueSerializerClass) {
        return createKafkaProperties(ByteArraySerializer.class, valueSerializerClass);
    }

    public static Properties createKafkaProperties(Class<?> keySerializerClass, Class<?> valueSerializerClass) {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getCanonicalName());
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass.getCanonicalName());
        return configProperties;
    }

    public static Properties createStreamsKafkaProperties(String applicationId) {
        Properties configProperties = new Properties();
        configProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        configProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return configProperties;
    }
}
