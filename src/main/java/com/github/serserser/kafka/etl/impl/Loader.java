package com.github.serserser.kafka.etl.impl;

import java.io.IOException;
import java.net.URISyntaxException;

public interface Loader {

    public void load() throws URISyntaxException, IOException;
}
