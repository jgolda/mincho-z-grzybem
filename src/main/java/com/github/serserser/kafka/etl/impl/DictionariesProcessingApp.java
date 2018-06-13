package com.github.serserser.kafka.etl.impl;

import java.io.IOException;
import java.net.URISyntaxException;

public class DictionariesProcessingApp {

    public static void main(String[] args) throws IOException, URISyntaxException {
        DictionariesProcessor dictionariesProcessor = new DictionariesProcessor();
        dictionariesProcessor.load();
        dictionariesProcessor.run();
    }
}
