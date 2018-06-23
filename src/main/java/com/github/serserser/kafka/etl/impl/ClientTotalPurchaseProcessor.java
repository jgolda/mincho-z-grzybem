package com.github.serserser.kafka.etl.impl;

import com.github.serserser.kafka.etl.impl.data.PurchasePricePair;
import com.github.serserser.kafka.etl.impl.serializers.CustomSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import static com.github.serserser.kafka.etl.impl.Topics.*;

public class ClientTotalPurchaseProcessor implements Runnable {

    public static final String APPLICATION_ID = "client-total-purchase-processor";

    public static void main(String[] args) {
        ClientTotalPurchaseProcessor processor = new ClientTotalPurchaseProcessor();
        processor.run();
    }

    @Override
    public void run() {
        StreamsBuilder builder = new StreamsBuilder();
        KTable<Integer, Double> commoditiesWithPrices = builder.table(COMMODITIES_KEY_PRICE_TOPIC_NAME, Consumed.with(Serdes.Integer(), Serdes.Double()));

        KTable<Integer, Double> purchasesByClientIds = builder.stream(PURCHASES_TOPIC_NAME, Consumed.with(Serdes.Integer(), CustomSerdes.purchase(), new WallclockTimestampExtractor(), Topology.AutoOffsetReset.EARLIEST))
                .map((key, value) -> new KeyValue<>(value.getCommodityId(), value))
                .join(commoditiesWithPrices, PurchasePricePair::new, Joined.with(Serdes.Integer(), CustomSerdes.purchase(), Serdes.Double()))
                .map((key, value) -> new KeyValue<>(value.getClientId(), value))
                .groupByKey(Serialized.with(Serdes.Integer(), CustomSerdes.purchasePricePair()))
                .aggregate(
                        () -> 0.0,
                        (key, value, aggregate) -> aggregate + value.getPrice() * value.getQuantity(),
                        Materialized.with(Serdes.Integer(), Serdes.Double())
                );

        purchasesByClientIds.toStream().to(PURCHASES_BY_CLIENTS_TOPIC_NAME, Produced.with(Serdes.Integer(), Serdes.Double()));

        KafkaStreams streams = new KafkaStreams(builder.build(), Utils.createStreamsKafkaProperties(APPLICATION_ID));
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
