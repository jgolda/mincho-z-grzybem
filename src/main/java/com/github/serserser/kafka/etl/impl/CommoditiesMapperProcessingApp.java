package com.github.serserser.kafka.etl.impl;

import java.io.IOException;
import java.net.URISyntaxException;

public class CommoditiesMapperProcessingApp {

    public static void main(String[] args) throws IOException, URISyntaxException {
        CommoditiesMapper commoditiesMapper = new CommoditiesMapper();
        commoditiesMapper.load();
        commoditiesMapper.run();
    }
}
