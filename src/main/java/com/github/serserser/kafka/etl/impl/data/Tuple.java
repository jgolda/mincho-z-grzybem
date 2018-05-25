package com.github.serserser.kafka.etl.impl.data;

public class Tuple<V1, V2> {

    private V1 firstValue;
    private V2 secondValue;

    public Tuple(V1 firstValue, V2 secondValue) {
        this.firstValue = firstValue;
        this.secondValue = secondValue;
    }

    public V1 getFirstValue() {
        return firstValue;
    }

    public V2 getSecondValue() {
        return secondValue;
    }
}
