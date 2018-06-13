package com.github.serserser.kafka.etl.impl;

import java.io.IOException;
import java.net.URISyntaxException;

public class ClientTotalPurchaseProcessingApp {

    public static void main(String[] args) throws IOException, URISyntaxException {
        ClientTotalPurchaseProcessor processor = new ClientTotalPurchaseProcessor();
        processor.load();
        processor.run();
    }
}
