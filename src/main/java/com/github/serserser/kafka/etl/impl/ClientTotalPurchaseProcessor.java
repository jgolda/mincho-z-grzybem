package com.github.serserser.kafka.etl.impl;

import com.github.serserser.kafka.etl.impl.data.Commodity;
import com.github.serserser.kafka.etl.impl.data.Purchase;
import com.github.serserser.kafka.etl.impl.data.PurchasePricePair;
import com.github.serserser.kafka.etl.impl.serializers.CustomSerdes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static com.github.serserser.kafka.etl.impl.Topics.COMMODITIES_TOPIC_NAME;
import static com.github.serserser.kafka.etl.impl.Topics.PURCHASES_BY_CLIENTS_TOPIC_NAME;
import static com.github.serserser.kafka.etl.impl.Topics.PURCHASES_TOPIC_NAME;

public class ClientTotalPurchaseProcessor implements Runnable, Loader {

    private static final boolean TEST_DATA_LOAD = false;

    @Override
    public void load() throws URISyntaxException, IOException {
        if ( TEST_DATA_LOAD ) {
            List<Commodity> commodities = Arrays.asList(
                    new Commodity(1, 5),
                    new Commodity(2, 2),
                    new Commodity(3, 10)
            );

            List<Purchase> purchases = Arrays.asList(
                    new Purchase(1, 1, 1, 1, 0),
                    new Purchase(2, 2, 3, 2, 0),
                    new Purchase(3, 3, 2, 4, 0),
                    new Purchase(4, 1, 3, 3, 0),
                    new Purchase(5, 1, 2, 2, 0),
                    new Purchase(6, 2, 1, 5, 0),
                    new Purchase(7, 2, 3, 2, 0),
                    new Purchase(8, 1, 1, 6, 0)
            );
            // clientid=1, total price = 69
            // clientid=2, total price = 65
            // clientid=3, total price = 8

            try ( Producer<String, Purchase> producer = new KafkaProducer<>(createKafkaProperties(CustomSerdes.purchase().getClass())) ) {
                purchases.forEach(purchase -> producer.send(new ProducerRecord<>(PURCHASES_TOPIC_NAME, purchase)));
            }

            try ( Producer<String, Commodity> producer = new KafkaProducer<>(createKafkaProperties(CustomSerdes.commodity().getClass())) ) {
                commodities.forEach(cmdty -> producer.send(new ProducerRecord<>(COMMODITIES_TOPIC_NAME, cmdty)));
            }
        } else {

            try ( Producer<String, Purchase> producer = new KafkaProducer<>(createKafkaProperties(CustomSerdes.purchase().getClass()));
                  Stream<String> purchaseStream = Files.lines(Paths.get(getClass().getClassLoader().getResource("data/purchases.txt").toURI()))) {
                purchaseStream.map(this::createPurchase)
                        .forEach(purchase -> producer.send(new ProducerRecord<>(PURCHASES_TOPIC_NAME, purchase)));
            }
            System.out.println("Loaded purchases to kafka");

            try ( Producer<String, Commodity> producer = new KafkaProducer<>(createKafkaProperties(CustomSerdes.commodity().getClass()));
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

    private Properties createKafkaProperties(Class<?> valueSerializerClass) {
        Properties configProperties = new Properties();
        configProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getCanonicalName());
        configProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, valueSerializerClass.getCanonicalName());
        return configProperties;
    }

    private Purchase createPurchase(String line) {
        String[] fields = line.split(",");
        return new Purchase(toInt(fields[0]), toInt(fields[1]), toInt(fields[2]), toInt(fields[3]), toInt(fields[4]));
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
        commoditiesWithPricesStream.to("polsl-temp-commodities-prices", Produced.with(Serdes.Integer(), Serdes.Double()));

        KTable<Integer, Double> commoditiesWithPrices = builder.table("polsl-temp-commodities-prices", Consumed.with(Serdes.Integer(), Serdes.Double()));

        KTable<Integer, Double> purchasesByClientIds = builder.stream(PURCHASES_TOPIC_NAME, Consumed.with(Serdes.Integer(), CustomSerdes.purchase(), new WallclockTimestampExtractor(), Topology.AutoOffsetReset.EARLIEST))
                .map((key, value) -> new KeyValue<>(value.getCommodityId(), value))
                .join(commoditiesWithPrices, (value1, value2) -> new PurchasePricePair(value1.getClientId(), value2, value1.getQuantity()), Joined.with(Serdes.Integer(), CustomSerdes.purchase(), Serdes.Double()))
                .map((key, value) -> new KeyValue<>(value.getClientId(), value))
                .groupByKey(Serialized.with(Serdes.Integer(), CustomSerdes.purchasePricePair()))
                .aggregate(
                        () -> 0.0,
                        (key, value, aggregate) -> aggregate + value.getPrice() * value.getQuantity(),
                        Materialized.with(Serdes.Integer(), Serdes.Double())
                );

        purchasesByClientIds.toStream().to(PURCHASES_BY_CLIENTS_TOPIC_NAME, Produced.with(Serdes.Integer(), Serdes.Double()));

        KafkaStreams streams = new KafkaStreams(builder.build(), createStreamsKafkaProperties());
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    private Properties createStreamsKafkaProperties() {
        Properties configProperties = new Properties();
        configProperties.put(StreamsConfig.APPLICATION_ID_CONFIG, "client-total-purchase-processor");
        configProperties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        return configProperties;
    }
}
