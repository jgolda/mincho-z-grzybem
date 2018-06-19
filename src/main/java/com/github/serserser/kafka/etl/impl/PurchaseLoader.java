package com.github.serserser.kafka.etl.impl;

import com.github.serserser.kafka.etl.impl.data.Purchase;
import com.github.serserser.kafka.etl.impl.serializers.CustomSerdes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.stream.Stream;

import static com.github.serserser.kafka.etl.impl.Topics.PURCHASES_TOPIC_NAME;

public class PurchaseLoader implements Loader {

    private static final Logger logger = LoggerFactory.getLogger(PurchaseLoader.class);

    public static final void main(String[] args) throws IOException, URISyntaxException {
        PurchaseLoader app = new PurchaseLoader();
        app.load();
    }

    @Override
    public void load() throws URISyntaxException, IOException {
        try ( Producer<String, Purchase> producer = new KafkaProducer<>(Utils.createKafkaProperties(CustomSerdes.purchase().getClass()));
              Stream<String> purchaseStream = Files.lines(Paths.get(getClass().getClassLoader().getResource("data/purchases.txt").toURI()))) {
            purchaseStream.map(this::createPurchase)
                    .forEach(purchase -> producer.send(new ProducerRecord<>(PURCHASES_TOPIC_NAME, purchase)));
        }
        logger.info("Loaded purchases to kafka");
    }

    private Purchase createPurchase(String line) {
        String[] fields = line.split(",");
        return new Purchase(toInt(fields[0]), toInt(fields[1]), toInt(fields[2]), toInt(fields[3]), toInt(fields[4]));
    }
}
