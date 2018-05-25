package com.github.serserser.kafka.etl.impl;

import org.junit.Test;

import static org.junit.Assert.*;

public class AverageTest {

    @Test
    public void shouldNotRoundValues() {
        double value = new Average(41, 4).value();
        assertEquals(10.25, value, 0.001);
    }

}