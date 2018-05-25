package com.github.serserser.kafka.etl.impl.data;

public class PurchasePricePair {

    private Purchase purchase = new Purchase();
    private Double price;

    public PurchasePricePair(Purchase purchase, Double price) {
        this.purchase = purchase;
        this.price = price;
    }

    public PurchasePricePair() {
    }

    public Integer getClientId() {
        return purchase.getClientId();
    }

    public void setClientId(Integer clientId) {
        purchase.setClientId(clientId);
    }

    public Double getPrice() {
        return price;
    }

    public void setPrice(Double price) {
        this.price = price;
    }

    public Integer getQuantity() {
        return purchase.getQuantity();
    }

    public void setQuantity(Integer quantity) {
        purchase.setQuantity(quantity);
    }

    public Integer getPosId() {
        return purchase.getPosId();
    }

    public void setPosId(Integer posId) {
        purchase.setPosId(posId);
    }
}
