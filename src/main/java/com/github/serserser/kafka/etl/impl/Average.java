package com.github.serserser.kafka.etl.impl;

public class Average {

    private double sum;
    private long amount;

    public Average(double sum, long amount) {
        this.sum = sum;
        this.amount = amount;
    }

    public Average() {
    }

    public Average accumulate(double nextValue) {
        return new Average(sum + nextValue, amount +1);
    }

    public double value() {
        return sum / amount;
    }

    public double getSum() {
        return sum;
    }

    public void setSum(double sum) {
        this.sum = sum;
    }

    public long getAmount() {
        return amount;
    }

    public void setAmount(long amount) {
        this.amount = amount;
    }
}
