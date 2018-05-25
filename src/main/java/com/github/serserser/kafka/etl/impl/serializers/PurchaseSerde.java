package com.github.serserser.kafka.etl.impl.serializers;

import com.github.serserser.kafka.etl.impl.data.Purchase;

public class PurchaseSerde extends JacksonSerde<Purchase> {
    public PurchaseSerde() {
        super(Purchase.class);
    }
}
