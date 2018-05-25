package com.github.serserser.kafka.etl.impl.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class JacksonSerde<T> implements Serializer<T>, Deserializer<T>, Serde<T> {

    public JacksonSerde(Class<T> serializedClass, boolean acceptNull) {
        this(serializedClass);
        this.acceptNull = acceptNull;
    }

    public JacksonSerde(Class<T> serializedClass) {
        this.serializedClass = serializedClass;
    }

    private Class<T> serializedClass;

    private boolean acceptNull = false;

    private ObjectMapper mapper = new ObjectMapper();

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
    }

    @Override
    public T deserialize(String topic, byte[] data) {
        if (data == null && acceptNull) {
            return null;
        }
        try {
            return mapper.readValue(data, serializedClass());
        } catch ( IOException e ) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            return mapper.writeValueAsBytes(data);
        } catch ( JsonProcessingException e ) {
            e.printStackTrace();
            return new byte[] {};
        }
    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }

    protected Class<T> serializedClass() {
        return serializedClass;
    }
}
