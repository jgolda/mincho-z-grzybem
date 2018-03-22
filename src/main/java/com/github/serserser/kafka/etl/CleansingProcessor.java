package com.github.serserser.kafka.etl;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class CleansingProcessor {

    public static void main(final String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "cleanser");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();
        KStream<String, String> textLines = builder.stream("kafka-etl");
        textLines.map((key, value) -> {
            System.out.println(value);
            return new KeyValue<>(key, value.toLowerCase());
        })
                .map((key, value) -> new KeyValue<>(key, value
                        .replace("!", "")
                        .replace(",", "")
                        .replace(".", "")
                        .replace("?", "")
                        .replace("-", "")
                        .replace(":", "")
                        .replace("\"", "")
                ))
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\n")))
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split("\t")))
                .flatMapValues(textLine -> Arrays.asList(textLine.toLowerCase().split(" ")))
                .to("filtered");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
