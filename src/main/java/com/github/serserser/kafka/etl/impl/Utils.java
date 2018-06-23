package com.github.serserser.kafka.etl.impl;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Properties;

public class Utils {

    public static final String DOCKER_LOCALHOST = "172.17.0.1";
    public static final String KAFKA_URL = DOCKER_LOCALHOST + ":9092";

    public static Properties createKafkaProperties(Class<?> valueSerializerClass) {
        return createKafkaProperties(ByteArraySerializer.class, valueSerializerClass);
    }

    public static Properties createKafkaProperties(Class<?> keySerializerClass, Class<?> valueSerializerClass) {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, keySerializerClass.getCanonicalName());
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass.getCanonicalName());
        return configProperties;
    }

    public static Properties createStreamsKafkaProperties(String applicationId) {
        Properties configProperties = new Properties();
        configProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        configProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_URL);
        return configProperties;
    }
}
