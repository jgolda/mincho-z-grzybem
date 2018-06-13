package com.github.serserser.kafka.etl.impl;

import com.github.serserser.kafka.etl.impl.data.Commodity;
import com.github.serserser.kafka.etl.impl.serializers.CustomSerdes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static com.github.serserser.kafka.etl.impl.Topics.COMMODITIES_KEY_PRICE_TOPIC_NAME;
import static com.github.serserser.kafka.etl.impl.Topics.COMMODITIES_TOPIC_NAME;

public class DictionariesProcessor implements Runnable, Loader {
    private static final boolean TEST_DATA_LOAD = true;
    private static final String APPLICATION_ID = "dictionaries-processor";

    @Override
    public void load() throws URISyntaxException, IOException {
        if ( TEST_DATA_LOAD ) {
            List<Commodity> commodities = Arrays.asList(
                    new Commodity(1, 5),
                    new Commodity(2, 2),
                    new Commodity(3, 10)
            );

            try ( Producer<String, Commodity> producer = new KafkaProducer<>(Utils.createKafkaProperties(CustomSerdes.commodity().getClass())) ) {
                commodities.forEach(cmdty -> producer.send(new ProducerRecord<>(COMMODITIES_TOPIC_NAME, cmdty)));
            }
        } else {
            try ( Producer<String, Commodity> producer = new KafkaProducer<>(Utils.createKafkaProperties(CustomSerdes.commodity().getClass()));
                  Stream<String> commoditiesStream = Files.lines(Paths.get(getClass().getClassLoader().getResource("data/commodities.txt").toURI()))) {
                commoditiesStream.map(this::createCommodity)
                        .forEach(cmdty -> producer.send(new ProducerRecord<>(COMMODITIES_TOPIC_NAME, cmdty)));
            }
            System.out.println("Loaded commodities to kafka");
        }
    }

    private Commodity createCommodity(String line) {
        String[] fields = line.split(",");
        return new Commodity(toInt(fields[0]), toDouble(fields[1]));
    }

    private Integer toInt(String str) {
        return Integer.valueOf(str);
    }

    private Double toDouble(String str) {
        return Double.valueOf(str);
    }

    @Override
    public void run() {
        StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, Double> commoditiesWithPricesStream = builder.stream(COMMODITIES_TOPIC_NAME, Consumed.with(Serdes.Integer(), CustomSerdes.commodity()))
                .map((key, value) -> new KeyValue<>(value.getCommodityId(), value.getPrice()));
        commoditiesWithPricesStream.to(COMMODITIES_KEY_PRICE_TOPIC_NAME, Produced.with(Serdes.Integer(), Serdes.Double()));

        KafkaStreams streams = new KafkaStreams(builder.build(), Utils.createStreamsKafkaProperties(APPLICATION_ID));
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
