package com.github.serserser.kafka.etl.impl;

import com.github.serserser.kafka.etl.impl.serializers.CustomSerdes;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.Serialized;

import static com.github.serserser.kafka.etl.impl.Topics.AVERAGE_PURCHASES_QUANTITY;
import static com.github.serserser.kafka.etl.impl.Topics.PURCHASES_TOPIC_NAME;

public class AveragePurchaseQuantityProcessor implements Runnable {

    private static final String APPLICATION_ID = "average-item-count-processor";

    public static void main(String [] args) {
        AveragePurchaseQuantityProcessor app = new AveragePurchaseQuantityProcessor();
        app.run();
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
