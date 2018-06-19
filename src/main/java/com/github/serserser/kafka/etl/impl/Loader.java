package com.github.serserser.kafka.etl.impl;

import java.io.IOException;
import java.net.URISyntaxException;

public interface Loader {

    public void load() throws URISyntaxException, IOException;

    default Integer toInt(String str) {
        return Integer.valueOf(str);
    }

    default Double toDouble(String str) {
        return Double.valueOf(str);
    }
}
