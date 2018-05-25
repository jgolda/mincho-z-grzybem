package com.github.serserser.kafka.etl.impl.serializers;

import com.github.serserser.kafka.etl.impl.Average;
import com.github.serserser.kafka.etl.impl.data.*;
import org.apache.kafka.common.serialization.Serde;

public class CustomSerdes {

    public static Serde<Commodity> commodity() {
        return new CommoditySerde();
    }

    public static Serde<Purchase> purchase() {
        return new PurchaseSerde();
    }

    public static Serde<PurchasePricePair> purchasePricePair() {
        return new JacksonSerde<>(PurchasePricePair.class);
    }

    public static Serde<Country> country() {
        return new CountrySerde();
    }

    public static Serde<PointOfSale> pointOfSale() {
        return new PointOfSaleSerde();
    }

    public static Serde<Tuple> tuple() {
        return new JacksonSerde<>(Tuple.class);
    }

    public static Serde<Average> average() {
        return new JacksonSerde<>(Average.class, true);
    }
}
