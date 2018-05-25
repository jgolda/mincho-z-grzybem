package com.github.serserser.kafka.etl.impl;

import java.io.IOException;
import java.net.URISyntaxException;

public class AveragePurchasesInCountriesProcessingApp {

    public static void main(String[] args) throws IOException, URISyntaxException {
        AveragePurchasesInCountriesProcessor processor = new AveragePurchasesInCountriesProcessor();
        processor.load();
        processor.run();
    }
}
