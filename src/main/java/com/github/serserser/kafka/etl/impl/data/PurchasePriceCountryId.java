package com.github.serserser.kafka.etl.impl.data;

public class PurchasePriceCountryId {

    private Integer countryId;
    private PurchasePricePair purchasePricePair;

    public PurchasePriceCountryId(Integer countryId, PurchasePricePair purchasePricePair) {
        this.countryId = countryId;
        this.purchasePricePair = purchasePricePair;
    }

    public Integer getCountryId() {
        return countryId;
    }

    public void setCountryId(Integer countryId) {
        this.countryId = countryId;
    }

    public PurchasePricePair getPurchasePricePair() {
        return purchasePricePair;
    }

    public void setPurchasePricePair(PurchasePricePair purchasePricePair) {
        this.purchasePricePair = purchasePricePair;
    }
}
