package com.github.serserser.kafka.etl.impl;

import com.github.serserser.kafka.etl.impl.data.Commodity;
import com.github.serserser.kafka.etl.impl.data.Purchase;
import com.github.serserser.kafka.etl.impl.data.PurchasePricePair;
import com.github.serserser.kafka.etl.impl.serializers.CustomSerdes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import static com.github.serserser.kafka.etl.impl.Topics.*;

public class ClientTotalPurchaseProcessor implements Runnable, Loader {

    private static final boolean TEST_DATA_LOAD = false;
    public static final String APPLICATION_ID = "client-total-purchase-processor";

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

            try ( Producer<String, Purchase> producer = new KafkaProducer<>(Utils.createKafkaProperties(CustomSerdes.purchase().getClass())) ) {
                purchases.forEach(purchase -> producer.send(new ProducerRecord<>(PURCHASES_TOPIC_NAME, purchase)));
            }

            try ( Producer<String, Commodity> producer = new KafkaProducer<>(Utils.createKafkaProperties(CustomSerdes.commodity().getClass())) ) {
                commodities.forEach(cmdty -> producer.send(new ProducerRecord<>(COMMODITIES_TOPIC_NAME, cmdty)));
            }
        }
    }

    private Commodity createCommodity(String line) {
        String[] fields = line.split(",");
        return new Commodity(toInt(fields[0]), toDouble(fields[1]));
    }

    private Purchase createPurchase(String line) {
        String[] fields = line.split(",");
        return new Purchase(toInt(fields[0]), toInt(fields[1]), toInt(fields[2]), toInt(fields[3]), toInt(fields[4]));
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
