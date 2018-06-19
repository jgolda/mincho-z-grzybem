package com.github.serserser.kafka.etl.impl;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.IntegerDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.stream.StreamSupport;

public class AveragePurchaseQuantityReceiver {

    public static void main(String[] args) throws InterruptedException {
        receive();
    }


    public static void receive() throws InterruptedException {
        try ( KafkaConsumer<Integer, Double> consumer = new KafkaConsumer<>(createKafkaProperties()) ) {
            consumer.subscribe(Collections.singletonList(Topics.AVERAGE_PURCHASES_QUANTITY));

            while ( true ) {
                ConsumerRecords<Integer, Double> records = consumer.poll(100);
                StreamSupport.stream(records.spliterator(), false)
                        .map(stringLongConsumerRecord -> "commodityId: " + stringLongConsumerRecord.key() + ";\t average quantity:" + stringLongConsumerRecord.value())
                        .forEach(System.out::println);
                Thread.sleep(100);
            }
        }
    }

    private static Properties createKafkaProperties() {
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getCanonicalName());
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class.getCanonicalName());
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-app");
        configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");
        return configProperties;
    }
}
