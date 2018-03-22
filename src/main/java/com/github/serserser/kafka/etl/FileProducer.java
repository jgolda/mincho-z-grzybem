package com.github.serserser.kafka.etl;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.Scanner;

public class FileProducer {

    public static void main(String[] args) {
        String topicName = args[0];
        String filename = args[1];
        Scanner scanner = null;
        try {
            scanner = new Scanner(new File(filename));
        } catch ( FileNotFoundException e ) {
            e.printStackTrace();
            return;
        }

        try ( Producer<String, String> producer = new KafkaProducer<>(createKafkaProperties()); ) {
            System.out.println("Enter message to be sent to Kafka. Type 'exit' to quit");

            while ( scanner.hasNext() ) {
                String line = scanner.next();
                ProducerRecord<String, String> record = new ProducerRecord<>(topicName, line);
                producer.send(record);
            }
        }
    }

    private static Properties createKafkaProperties() {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getCanonicalName());
        return configProperties;
    }
}
