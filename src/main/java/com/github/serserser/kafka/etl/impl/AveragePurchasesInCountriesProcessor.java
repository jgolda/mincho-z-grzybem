package com.github.serserser.kafka.etl.impl;

import com.github.serserser.kafka.etl.impl.data.*;
import com.github.serserser.kafka.etl.impl.serializers.CustomSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import static com.github.serserser.kafka.etl.impl.Topics.*;

public class AveragePurchasesInCountriesProcessor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(AveragePurchasesInCountriesProcessor.class);

    private static final String APPLICATION_ID = "country-average-purchase-processor";

    public static void main(String[] args) {
        logger.info("starting average purchases app...");
        AveragePurchasesInCountriesProcessor processor = new AveragePurchasesInCountriesProcessor();
        processor.run();
    }

    @Override
    public void run() {
        StreamsBuilder builder = new StreamsBuilder();

        KTable<Integer, Double> commoditiesWithPrices = builder.table(COMMODITIES_KEY_PRICE_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Double()));

        KTable<Integer, Integer> pointOfSales = builder.table(POINT_OF_SALE_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Integer()));

        KTable<Integer, Country> countries = builder.table(COUNTRIES_TOPIC_NAME, Consumed.with(Serdes.Integer(), CustomSerdes.country()));

        builder.stream(PURCHASES_TOPIC_NAME, Consumed.with(Serdes.Integer(), CustomSerdes.purchase()))
                .map((key, value) -> {
                    logger.info("map1. Id: " + value.getPurchaseId());
                    return new KeyValue<>(value.getCommodityId(), value);
                })
                .join(commoditiesWithPrices, PurchasePricePair::new, Joined.with(Serdes.Integer(), CustomSerdes.purchase(), Serdes.Double()))
                .map((key, value) -> {
                    logger.info("map2. Id: " + value.getPosId());
                    return new KeyValue<>(value.getPosId(), value);
                })
                .join(pointOfSales, (purchasePrice, countryId) -> new PurchasePriceCountryId(countryId, purchasePrice), Joined.with(Serdes.Integer(), CustomSerdes.purchasePricePair(), Serdes.Integer()))
                .map((key, value) -> {
                    logger.info("map3. Id: " + value.getPurchasePricePair().getPosId());
                    return new KeyValue<>(value.getCountryId(), value.getPurchasePricePair());
                })
                .groupByKey(Serialized.with(Serdes.Integer(), CustomSerdes.purchasePricePair()))
                .aggregate(() -> new Average(0.0, 0),
                        ((key, value, aggregate) -> {
                            logger.info("aggregate. posId: " + value.getPosId() + " cliId: " + value.getClientId());
                            return aggregate.accumulate(value.getQuantity() * value.getPrice());
                        }),
                        Materialized.with(Serdes.Integer(), CustomSerdes.average()))
                .mapValues(value -> {
                            logger.info("mapValues of aggregate");
                            return value.value();
                        },
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
