package com.github.serserser.kafka.etl.impl;

import com.github.serserser.kafka.etl.impl.data.Purchase;
import com.github.serserser.kafka.etl.impl.serializers.CustomSerdes;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

import static com.github.serserser.kafka.etl.impl.Topics.PURCHASES_TOPIC_NAME;

public class PurchaseLoader implements Loader {

    private static final Logger logger = LoggerFactory.getLogger(PurchaseLoader.class);
    private static final boolean TEST_DATA_LOAD = Boolean.parseBoolean(System.getenv().getOrDefault("TEST_LOAD", "false"));

    public static final void main(String[] args) throws IOException, URISyntaxException {
        PurchaseLoader app = new PurchaseLoader();
        app.load();
    }

    @Override
    public void load() throws URISyntaxException, IOException {
        if (TEST_DATA_LOAD) {
            testLoad();
        } else {
            URI uri = getClass().getClassLoader().getResource("data/purchases.txt").toURI();
            String[] uriElements = uri.toString().split("!");
            logger.info("Started loading purchases to Kafka");
            try ( Producer<String, Purchase> producer = new KafkaProducer<>(Utils.createKafkaProperties(CustomSerdes.purchase().getClass()));
                  FileSystem fs = getFileSystem(uriElements[0]);
                  Stream<String> purchaseStream = Files.lines(fs.getPath(uriElements[1])) ) {
                purchaseStream.map(this::createPurchase)
                        .forEach(purchase -> producer.send(new ProducerRecord<>(PURCHASES_TOPIC_NAME, purchase)));
            }
            logger.info("Loaded purchases to kafka");
        }
    }

    private void testLoad() {
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

        try ( Producer<String, Purchase> producer = new KafkaProducer<>(Utils.createKafkaProperties(CustomSerdes.purchase().getClass())) ) {
            purchases.forEach(purchase -> producer.send(new ProducerRecord<>(PURCHASES_TOPIC_NAME, purchase)));
        }
    }

    private FileSystem getFileSystem(String uriElement) throws IOException {
        URI uri = URI.create(uriElement);
        try {
            return FileSystems.getFileSystem(uri);
        } catch ( FileSystemNotFoundException e ) {
            return FileSystems.newFileSystem(uri, new HashMap<>());
        }
    }

    private Purchase createPurchase(String line) {
        String[] fields = line.split(",");
        return new Purchase(toInt(fields[0]), toInt(fields[1]), toInt(fields[2]), toInt(fields[3]), toInt(fields[4]));
    }
}
