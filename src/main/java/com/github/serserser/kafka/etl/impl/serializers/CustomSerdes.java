package com.github.serserser.kafka.etl.impl.serializers;

import com.github.serserser.kafka.etl.impl.data.Commodity;
import com.github.serserser.kafka.etl.impl.data.Country;
import com.github.serserser.kafka.etl.impl.data.Purchase;
import com.github.serserser.kafka.etl.impl.data.PurchasePricePair;
import org.apache.kafka.common.serialization.Serde;

public class CustomSerdes {

    public static Serde<Country> country() {
        return new JacksonSerde<>(Country.class);
    }

    public static Serde<Commodity> commodity() {
        return new CommoditySerde();
    }

    public static Serde<Purchase> purchase() {
        return new PurchaseSerde();
    }

    public static Serde<PurchasePricePair> purchasePricePair() {
        return new JacksonSerde<>(PurchasePricePair.class);
    }

}
