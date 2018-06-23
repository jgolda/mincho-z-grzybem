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
import java.util.HashMap;
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
        URI uri = getClass().getClassLoader().getResource("data/purchases.txt").toURI();
        String[] uriElements = uri.toString().split("!");
        logger.info("Started loading purchases to Kafka");
        try ( Producer<String, Purchase> producer = new KafkaProducer<>(Utils.createKafkaProperties(CustomSerdes.purchase().getClass()));
              FileSystem fs = getFileSystem(uriElements[0]);
              Stream<String> purchaseStream = Files.lines(fs.getPath(uriElements[1]))) {
            purchaseStream.map(this::createPurchase)
                    .forEach(purchase -> producer.send(new ProducerRecord<>(PURCHASES_TOPIC_NAME, purchase)));
        }
        logger.info("Loaded purchases to kafka");
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
