package com.github.serserser.kafka.etl.impl;

import java.io.IOException;
import java.net.URISyntaxException;

public class AveragePurchaseQuantityProcessingApp {

    public static void main(String [] args) throws IOException, URISyntaxException {
        AveragePurchaseQuantityProcessor app = new AveragePurchaseQuantityProcessor();
        app.load();
        app.run();
    }
}
