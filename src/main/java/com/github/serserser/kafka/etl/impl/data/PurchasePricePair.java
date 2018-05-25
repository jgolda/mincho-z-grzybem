package com.github.serserser.kafka.etl.impl.data;

public class PurchasePricePair {

    private Integer clientId;
    private Double price;
    private Integer quantity;

    public PurchasePricePair(Integer clientId, Double price, Integer quantity) {
        this.clientId = clientId;
        this.price = price;
        this.quantity = quantity;
    }

    public PurchasePricePair() {
    }

    public Integer getClientId() {
        return clientId;
    }

    public Double getPrice() {
        return price;
    }

    public Integer getQuantity() {
        return quantity;
    }

    public void setClientId(Integer clientId) {
        this.clientId = clientId;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public void setQuantity(Integer quantity) {
        this.quantity = quantity;
    }
}
