package com.github.serserser.kafka.etl.impl;

import org.slf4j.Logger;

public abstract class ElapsedTimeCalculator implements Runnable {

    private boolean running = false;
    private long startDate = -1;
    private long endDate = -1;
    private long possibleEndDate = -1;
    private int attempt = 0;

    @Override
    public void run() {
        boolean currentRunning = running;
        if (currentRunning && startDate < 0) {
            startDate = System.currentTimeMillis();
            logger().info("started processing in time: " + startDate);
        } else if (!currentRunning && startDate > 0 && endDate < 0 && attempt < 20) {
            attempt++;
            logger().info("found attempt: " + attempt);
            if (attempt == 1) {
                possibleEndDate = System.currentTimeMillis();
                logger().info("possible end date: " + possibleEndDate);
            }
        } else if (!currentRunning && startDate > 0 && endDate < 0) {
            endDate = possibleEndDate;
            long elapsedMillis = endDate - startDate;
            double elapsedSeconds = elapsedMillis / 1000.0 / 60;
            logger().info("finished processing in time: " + endDate);
            logger().info("total elapsed time in seconds: " + elapsedSeconds);
        }
        if (currentRunning && attempt > 0) {
            attempt = 0;
        }
        running = false;
    }

    protected abstract Logger logger();
}
