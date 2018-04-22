package com.github.serserser.kafka.generator;

public class Commodity {

    private int commodityId;
    private double price;

    public Commodity(int commodityId, double price) {
        this.commodityId = commodityId;
        this.price = price;
    }

    public int getCommodityId() {
        return commodityId;
    }

    public double getPrice() {
        return price;
    }
}
