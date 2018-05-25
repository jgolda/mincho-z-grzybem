package com.github.serserser.kafka.etl.impl.serializers;

import com.github.serserser.kafka.etl.impl.data.Country;

public class CountrySerde extends JacksonSerde<Country> {

    public CountrySerde() {
        super(Country.class, true);
    }
}
