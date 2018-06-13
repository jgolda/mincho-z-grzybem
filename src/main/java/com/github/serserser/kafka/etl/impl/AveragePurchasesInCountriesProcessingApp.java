package com.github.serserser.kafka.etl.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URISyntaxException;

public class AveragePurchasesInCountriesProcessingApp {

    private static final Logger logger = LoggerFactory.getLogger(AveragePurchasesInCountriesProcessingApp.class);

    public static void main(String[] args) throws IOException, URISyntaxException {
        logger.info("starting average purchases app...");
        AveragePurchasesInCountriesProcessor processor = new AveragePurchasesInCountriesProcessor();
        processor.load();
        processor.run();
    }
}
