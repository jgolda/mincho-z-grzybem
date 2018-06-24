package com.github.serserser.kafka.etl.impl;

import org.slf4j.Logger;

import java.time.Duration;
import java.time.Instant;

public abstract class ElapsedTimeCalculator implements Runnable {

    private boolean running = false;
    private Instant startDate = Instant.MIN;
    private Instant endDate = Instant.MIN;
    private Instant possibleEndDate = Instant.MIN;
    private int attempt = 0;

    @Override
    public void run() {
        boolean currentRunning = running;
        if (currentRunning && notStarted(startDate) ) {
            startDate = Instant.now();
            logger().info("started processing in time: " + time(startDate));
        } else if (!currentRunning && started(startDate) && notStarted(endDate) && attempt < 20) {
            attempt++;
            logger().info("found attempt: " + attempt);
            if (attempt == 1) {
                possibleEndDate = Instant.now();
                logger().info("possible end date: " + time(possibleEndDate));
            }
        } else if (!currentRunning && started(startDate) && notStarted(endDate)) {
            endDate = possibleEndDate;
            double elapsedSeconds = Duration.between(startDate, endDate).toSeconds();
            logger().info("finished processing in time: " + time(endDate));
            logger().info("total elapsed time in seconds: " + elapsedSeconds);
        }
        if (currentRunning && attempt > 0) {
            attempt = 0;
        }
        running = false;
    }

    private String time(Instant date) {
        return date.getEpochSecond() + "." + date.getNano();
    }

    private boolean started(Instant instant) {
        return ! notStarted(instant);
    }

    private boolean notStarted(Instant startDate) {
        return Instant.MIN.equals(startDate);
    }

    protected abstract Logger logger();
}
