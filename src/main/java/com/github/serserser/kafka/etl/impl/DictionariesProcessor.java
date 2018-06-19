package com.github.serserser.kafka.etl.impl;

import com.github.serserser.kafka.etl.impl.data.Commodity;
import com.github.serserser.kafka.etl.impl.data.Country;
import com.github.serserser.kafka.etl.impl.data.PointOfSale;
import com.github.serserser.kafka.etl.impl.serializers.CustomSerdes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static com.github.serserser.kafka.etl.impl.Topics.*;

public class DictionariesProcessor implements Runnable, Loader {

    private static final Logger logger = LoggerFactory.getLogger(DictionariesProcessor.class);

    private static final String APPLICATION_ID = "dictionaries-processor";

    @Override
    public void load() throws URISyntaxException, IOException {
        try ( Producer<String, Country> producer = new KafkaProducer<>(Utils.createKafkaProperties(CustomSerdes.country().getClass()));
              Stream<String> countries = Files.lines(Paths.get(getClass().getClassLoader().getResource("data/countries.txt").toURI())) ) {
            countries.map(this::createCountry)
                    .forEach(cmdty -> producer.send(new ProducerRecord<>(COUNTRIES_TOPIC_NAME, cmdty)));
        }

        logger.info("Loaded countries (1/3)");


        try ( Producer<String, Commodity> producer = new KafkaProducer<>(Utils.createKafkaProperties(CustomSerdes.commodity().getClass()));
              Stream<String> commodities = Files.lines(Paths.get(getClass().getClassLoader().getResource("data/commodities.txt").toURI())) ) {
            commodities.map(this::createCommodity)
                    .forEach(cmdty -> producer.send(new ProducerRecord<>(COMMODITIES_TOPIC_NAME, cmdty)));
        }

        logger.info("Loaded commodities (2/3)");


        try ( Producer<Integer, Integer> producer = new KafkaProducer<>(Utils.createKafkaProperties(IntegerSerializer.class, IntegerSerializer.class));
            Stream<String> pointsOfSale = Files.lines(Paths.get(getClass().getClassLoader().getResource("data/pointsOfSale.txt").toURI())) ) {
            pointsOfSale.map(this::createPointOfSale)
                    .forEach(cmdty -> producer.send(new ProducerRecord<>(POINT_OF_SALE_TOPIC_NAME, cmdty.getShopId(), cmdty.getCountryId())));
        }

        logger.info("Loaded points of sale (3/3)");


        logger.info("Loaded all data");
    }

    private PointOfSale createPointOfSale(String line) {
        String[] fields = line.split(",");
        return new PointOfSale(toInt(fields[0]), toInt(fields[1]));
    }

    private Country createCountry(String line) {
        String[] fields = line.split(",");
        return new Country(toInt(fields[0]), fields[1], fields[2]);
    }

    private Commodity createCommodity(String line) {
        String[] fields = line.split(",");
        return new Commodity(toInt(fields[0]), toDouble(fields[1]));
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
