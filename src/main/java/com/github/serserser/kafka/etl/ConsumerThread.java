package com.github.serserser.kafka.etl;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class ConsumerThread extends Thread {

    private final String topicName;

    private final String groupId;

    private KafkaConsumer<String, Long> consumer;

    public ConsumerThread(String topicName, String groupId) {
        this.topicName = topicName;
        this.groupId = groupId;
    }

    @Override
    public void run() {
        try {
            consumer = new KafkaConsumer<>(createKafkaProperties());
            consumer.subscribe(Collections.singletonList(topicName));
            while(true) {
                ConsumerRecords<String, Long> records = consumer.poll(100);
                StreamSupport.stream(records.spliterator(), false)
                        .map(stringLongConsumerRecord -> "" + stringLongConsumerRecord.key() + ": " + stringLongConsumerRecord.value())
                        .forEach(System.out::println);
            }
        } catch ( WakeupException exc ) {
            System.out.println("Caught wakeup exception: " + exc.getMessage());
        } finally {
            consumer.close();
        }
    }

    public KafkaConsumer<String,Long> getKafkaConsumer(){
        return consumer;
    }

    private Properties createKafkaProperties() {
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getCanonicalName());
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");
        return configProperties;
    }
}
