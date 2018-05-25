package com.github.serserser.kafka.etl.impl.serializers;

import com.github.serserser.kafka.etl.impl.data.Commodity;

public class CommoditySerde extends JacksonSerde<Commodity> {
    public CommoditySerde() {
        super(Commodity.class);
    }
}
