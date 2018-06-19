package com.github.serserser.kafka.etl.impl;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.DoubleDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;
import java.util.stream.StreamSupport;

public class AveragePurchasesInCountriesReceiver {

    public static void main(String[] args) throws InterruptedException {
        receive();
    }

    public static void receive() throws InterruptedException {
        try ( KafkaConsumer<String, Double> consumer = new KafkaConsumer<>(createKafkaProperties()) ) {
            consumer.subscribe(Collections.singletonList(Topics.AVERAGE_PURCHASES_BY_COUNTRIES));
            while ( true ) {
                ConsumerRecords<String, Double> records = consumer.poll(100);
                StreamSupport.stream(records.spliterator(), false)
                        .map(record -> "countryCode: " + record.key() + ";\t average purchase:" + record.value())
                        .forEach(System.out::println);
                Thread.sleep(100);
            }
        }
    }


    private static Properties createKafkaProperties() {
        Properties configProperties = new Properties();
        configProperties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getCanonicalName());
        configProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, DoubleDeserializer.class.getCanonicalName());
        configProperties.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-app");
        configProperties.put(ConsumerConfig.CLIENT_ID_CONFIG, "simple");
        return configProperties;
    }

}

