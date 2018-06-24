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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static com.github.serserser.kafka.etl.impl.Topics.AVERAGE_PURCHASES_QUANTITY;
import static com.github.serserser.kafka.etl.impl.Topics.PURCHASES_TOPIC_NAME;

public class AveragePurchaseQuantityProcessor implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(AveragePurchaseQuantityProcessor.class);

    private static final String APPLICATION_ID = "average-item-count-processor";
    private static ScheduledExecutorService executor = Executors.newScheduledThreadPool(2);

    private boolean running = false;
    private Instant startDate = Instant.MIN;
    private Instant endDate = Instant.MIN;
    private Instant possibleEndDate = Instant.MIN;
    private int attempt = 0;

    public static void main(String[] args) {
        AveragePurchaseQuantityProcessor app = new AveragePurchaseQuantityProcessor();
        executor.scheduleAtFixedRate(app, 1, 2, TimeUnit.SECONDS);
        app.execute();
    }

    public void execute() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(PURCHASES_TOPIC_NAME, Consumed.with(Serdes.Integer(), CustomSerdes.purchase()))
                .map((key, value) -> {
                    running = true;
                    return new KeyValue<>(value.getCommodityId(), value.getQuantity());
                })
                .groupByKey(Serialized.with(Serdes.Integer(), Serdes.Integer()))
                .aggregate(() -> new Average(0.0, 0),
                        (key, value, aggregate) -> {
                            running = true;
                            return aggregate.accumulate(value);
                        },
                        Materialized.with(Serdes.Integer(), CustomSerdes.average()))
                .mapValues(value -> {
                            running = true;
                            return value.value();
                        },
                        Materialized.with(Serdes.Integer(), Serdes.Double()))
                .toStream()
                .to(AVERAGE_PURCHASES_QUANTITY, Produced.with(Serdes.Integer(), Serdes.Double()));

        KafkaStreams streams = new KafkaStreams(builder.build(), Utils.createStreamsKafkaProperties(APPLICATION_ID));
        streams.cleanUp();
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
        Runtime.getRuntime().addShutdownHook(new Thread(executor::shutdown));
    }

    @Override
    public void run() {
        boolean currentRunning = running;
        if (currentRunning && notStarted(startDate) ) {
            startDate = Instant.now();
            logger.info("started processing in time: " + time(startDate));
        } else if (!currentRunning && started(startDate) && notStarted(endDate) && attempt < 20) {
            attempt++;
            logger.info("found attempt: " + attempt);
            if (attempt == 1) {
                possibleEndDate = Instant.now();
                logger.info("possible end date: " + time(possibleEndDate));
            }
        } else if (!currentRunning && started(startDate) && notStarted(endDate)) {
            endDate = possibleEndDate;
            double elapsedSeconds = Duration.between(startDate, endDate).toSeconds();
            logger.info("finished processing in time: " + time(endDate));
            logger.info("total elapsed time in seconds: " + elapsedSeconds);
        }
        if (currentRunning && attempt > 0) {
            attempt = 0;
        }
        running = false;
    }

    private String time(Instant date) {
        return date.getEpochSecond() + "." + date.getNano();
    }

    private boolean started(Instant instant) {
        return ! notStarted(instant);
    }

    private boolean notStarted(Instant startDate) {
        return Instant.MIN.equals(startDate);
    }
}