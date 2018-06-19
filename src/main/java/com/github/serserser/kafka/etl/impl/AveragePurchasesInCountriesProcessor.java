package com.github.serserser.kafka.etl.impl;

import com.github.serserser.kafka.etl.impl.data.*;
import com.github.serserser.kafka.etl.impl.serializers.CustomSerdes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import static com.github.serserser.kafka.etl.impl.Topics.*;

public class AveragePurchasesInCountriesProcessor implements Runnable, Loader {

    private static final boolean TEST_DATA_LOAD = false;
    private static final String APPLICATION_ID = "country-average-purchase-processor";

    @Override
    public void load() throws URISyntaxException, IOException {
        if (TEST_DATA_LOAD) {
            List<Country> countries = Arrays.asList(
                    new Country(1, "PL", "Polandia"),
                    new Country(2, "DE", "Niemcy"),
                    new Country(3, "USA", "Burgerlandia")
            );

            List<PointOfSale> pointsOfSale = Arrays.asList(
                    new PointOfSale(1, 1),
                    new PointOfSale(2, 1),
                    new PointOfSale(3, 3),
                    new PointOfSale(4, 2),
                    new PointOfSale(5, 1),
                    new PointOfSale(6, 2)
            );
//            PL - 1, 2, 5 = 47 / 6 = 7.8333
//            DE - 4, 6 = 43 / 4 = 10.75
//            USA - 3 = 104 / 2 = 52

            List<Commodity> commodities = Arrays.asList(
                    new Commodity(1, 1),
                    new Commodity(2, 2),
                    new Commodity(3, 5),
                    new Commodity(4, 10)
            );

            List<Purchase> purchases = Arrays.asList(
                    new Purchase(1, 1, 1, 4, 1),
                    new Purchase(2, 1, 3, 5, 2),
                    new Purchase(3, 1, 2, 2, 3),
                    new Purchase(4, 1, 3, 1, 4),
                    new Purchase(5, 1, 2, 2, 5),
                    new Purchase(6, 1, 4, 3, 6),
                    new Purchase(7, 1, 2, 1, 1),
                    new Purchase(8, 1, 1, 7, 1),
                    new Purchase(9, 1, 4, 10, 3),
                    new Purchase(10, 1, 1, 2, 4),
                    new Purchase(11, 1, 3, 1, 5),
                    new Purchase(12, 1, 2, 3, 6)
            );
//            pos 1 = 13
//            pos 2 = 25
//            pos 3 = 104
//            pos 4 = 5
//            pos 5 = 9
//            pos 6 = 36

            try ( Producer<Integer, Country> producer = new KafkaProducer<>(Utils.createKafkaProperties(IntegerSerializer.class, CustomSerdes.country().getClass())) ) {
                countries.forEach(country -> producer.send(new ProducerRecord<Integer, Country>(COUNTRIES_TOPIC_NAME, country.getId(), country)));
            }

            try ( Producer<Integer, Integer> producer = new KafkaProducer<>(Utils.createKafkaProperties(IntegerSerializer.class, IntegerSerializer.class)) ) {
                pointsOfSale.forEach(pointOfSale -> producer.send(new ProducerRecord<Integer, Integer>(POINT_OF_SALE_TOPIC_NAME, pointOfSale.getShopId(), pointOfSale.getCountryId())));
            }

            try ( Producer<String, Commodity> producer = new KafkaProducer<>(Utils.createKafkaProperties(CustomSerdes.commodity().getClass())) ) {
                commodities.forEach(cmdty -> producer.send(new ProducerRecord<>(COMMODITIES_TOPIC_NAME, cmdty)));
            }

            try ( Producer<String, Purchase> producer = new KafkaProducer<>(Utils.createKafkaProperties(CustomSerdes.purchase().getClass())) ) {
                purchases.forEach(purchase -> producer.send(new ProducerRecord<>(PURCHASES_TOPIC_NAME, purchase)));
            }
        }
    }

    @Override
    public void run() {
        StreamsBuilder builder = new StreamsBuilder();

        KTable<Integer, Double> commoditiesWithPrices = builder.table(COMMODITIES_KEY_PRICE_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Double()));

        KTable<Integer, Integer> pointOfSales = builder.table(POINT_OF_SALE_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()));

        KTable<Integer, Country> countries = builder.table(COUNTRIES_TOPIC_NAME, Consumed.with(Serdes.Integer(), CustomSerdes.country()));

        builder.stream(PURCHASES_TOPIC_NAME, Consumed.with(Serdes.Integer(), CustomSerdes.purchase()))
                .map((key, value) -> new KeyValue<>(value.getCommodityId(), value))
                .join(commoditiesWithPrices, PurchasePricePair::new, Joined.with(Serdes.Integer(), CustomSerdes.purchase(), Serdes.Double()))
                .map((key, value) -> new KeyValue<>(value.getPosId(), value))
                .join(pointOfSales, (purchasePrice, countryId) -> new PurchasePriceCountryId(countryId, purchasePrice), Joined.with(Serdes.Integer(), CustomSerdes.purchasePricePair(), Serdes.Integer()))
                .map((key, value) -> new KeyValue<>(value.getCountryId(), value.getPurchasePricePair()))
                .groupByKey(Serialized.with(Serdes.Integer(), CustomSerdes.purchasePricePair()))
                .aggregate(() -> new Average(0.0, 0),
                        ((key, value, aggregate) -> aggregate.accumulate(value.getQuantity() * value.getPrice())),
                        Materialized.with(Serdes.Integer(), CustomSerdes.average()))
                .mapValues(value -> value.value(),
                        Materialized.with(Serdes.Integer(), Serdes.Double()))
                .toStream()
                .join(countries, (sale, country) -> new Tuple<>(country.getCode(), sale), Joined.with(Serdes.Integer(), Serdes.Double(), CustomSerdes.country()))
                .map((key, value) -> new KeyValue<>(value.getFirstValue(), value.getSecondValue()))
                .to(AVERAGE_PURCHASES_BY_COUNTRIES, Produced.with(Serdes.String(), Serdes.Double()));

        KafkaStreams streams = new KafkaStreams(builder.build(), Utils.createStreamsKafkaProperties(APPLICATION_ID));
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
