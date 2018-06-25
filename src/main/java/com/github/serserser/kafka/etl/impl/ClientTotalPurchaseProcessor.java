package com.github.serserser.kafka.etl.impl;

import com.github.serserser.kafka.etl.impl.data.PurchasePricePair;
import com.github.serserser.kafka.etl.impl.serializers.CustomSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.github.serserser.kafka.etl.impl.Topics.*;

public class ClientTotalPurchaseProcessor extends ElapsedTimeCalculator {

    private static final Logger logger = LoggerFactory.getLogger(ClientTotalPurchaseProcessor.class);
    public static final String APPLICATION_ID = "client-total-purchase-processor";
    private static ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

    public static void main(String[] args) {
        ClientTotalPurchaseProcessor app = new ClientTotalPurchaseProcessor();
        executor.scheduleAtFixedRate(app, 1, 2, TimeUnit.SECONDS);
        app.execute();
    }

    public void execute() {
        StreamsBuilder builder = new StreamsBuilder();
        KTable<Integer, Double> commoditiesWithPrices = builder.table(COMMODITIES_KEY_PRICE_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Double()));

        KTable<Integer, Double> purchasesByClientIds = builder.stream(PURCHASES_TOPIC_NAME, Consumed.with(Serdes.Integer(), CustomSerdes.purchase(), new WallclockTimestampExtractor(), Topology.AutoOffsetReset.EARLIEST))
                .map((key, value) -> {
                    heartBeat();
                    return new KeyValue<>(value.getCommodityId(), value);
                })
                .join(commoditiesWithPrices, (purchase, price) -> {
                    heartBeat();
                    return new PurchasePricePair(purchase, price);
                }, Joined.with(Serdes.Integer(), CustomSerdes.purchase(), Serdes.Double()))
                .map((key, value) -> {
                    heartBeat();
                    return new KeyValue<>(value.getClientId(), value);
                })
                .groupByKey(Serialized.with(Serdes.Integer(), CustomSerdes.purchasePricePair()))
                .aggregate(
                        () -> 0.0,
                        (key, value, aggregate) -> {
                            heartBeat();
                            return aggregate + value.getPrice() * value.getQuantity();
                        },
                        Materialized.with(Serdes.Integer(), Serdes.Double())
                );

        purchasesByClientIds.toStream().to(PURCHASES_BY_CLIENTS_TOPIC_NAME, Produced.with(Serdes.Integer(), Serdes.Double()));

        KafkaStreams streams = new KafkaStreams(builder.build(), Utils.createStreamsKafkaProperties(APPLICATION_ID));
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

    @Override
    protected Logger logger() {
        return logger;
    }
}
