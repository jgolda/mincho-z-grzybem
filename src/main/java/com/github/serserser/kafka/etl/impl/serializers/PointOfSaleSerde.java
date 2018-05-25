package com.github.serserser.kafka.etl.impl.serializers;

import com.github.serserser.kafka.etl.impl.data.PointOfSale;

public class PointOfSaleSerde extends JacksonSerde<PointOfSale> {
    public PointOfSaleSerde() {
        super(PointOfSale.class);
    }
}
