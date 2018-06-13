package com.github.serserser.kafka.etl.impl;

import com.github.serserser.kafka.etl.impl.data.Purchase;
import com.github.serserser.kafka.etl.impl.serializers.CustomSerdes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.List;

import static com.github.serserser.kafka.etl.impl.Topics.AVERAGE_PURCHASES_QUANTITY;
import static com.github.serserser.kafka.etl.impl.Topics.PURCHASES_TOPIC_NAME;

public class AveragePurchaseQuantityProcessor implements Loader, Runnable {

    private static final boolean TEST_DATA_LOAD = true;
    private static final String APPLICATION_ID = "average-item-count-processor";

    @Override
    public void load() throws URISyntaxException, IOException {
        if (TEST_DATA_LOAD) {
            List<Purchase> purchases = Arrays.asList(
                    new Purchase(1, 1, 1, 2, 1),
                    new Purchase(2, 1, 2, 4, 1),
                    new Purchase(3, 1, 2, 1, 1),
                    new Purchase(4, 1, 3, 6, 1),
                    new Purchase(5, 1, 1, 7, 1),
                    new Purchase(6, 1, 2, 3, 1),
                    new Purchase(7, 1, 3, 1, 1),
                    new Purchase(8, 1, 2, 4, 1),
                    new Purchase(9, 1, 4, 5, 1),
                    new Purchase(10, 1, 2, 13, 1),
                    new Purchase(11, 1, 1, 1, 1),
                    new Purchase(12, 1, 2, 8, 1),
                    new Purchase(13, 1, 3, 2, 1)
            );
//            commodityId: average quantity
//            1: 3.333
//            2: 5.5
//            3: 3
//            4: 5

            try ( Producer<String, Purchase> producer = new KafkaProducer<>(Utils.createKafkaProperties(CustomSerdes.purchase().getClass())) ) {
                purchases.forEach(purchase -> producer.send(new ProducerRecord<>(PURCHASES_TOPIC_NAME, purchase)));
            }
        }
    }

    @Override
    public void run() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(PURCHASES_TOPIC_NAME, Consumed.with(Serdes.Integer(), CustomSerdes.purchase()))
                .map((key, value) -> new KeyValue<>(value.getCommodityId(), value.getQuantity()))
                .groupByKey(Serialized.with(Serdes.Integer(), Serdes.Integer()))
                .aggregate(() -> new Average(0.0, 0),
                        (key, value, aggregate) -> aggregate.accumulate(value),
                        Materialized.with(Serdes.Integer(), CustomSerdes.average()))
                .mapValues(value -> value.value(),
                        Materialized.with(Serdes.Integer(), Serdes.Double()))
                .toStream()
                .to(AVERAGE_PURCHASES_QUANTITY, Produced.with(Serdes.Integer(), Serdes.Double()));

        KafkaStreams streams = new KafkaStreams(builder.build(), Utils.createStreamsKafkaProperties(APPLICATION_ID));
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
