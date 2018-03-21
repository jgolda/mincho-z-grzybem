package com.github.serserser.kafka.etl;

import java.util.Scanner;

public class ConsoleConsumerApp {

    public static void main(String[] args) throws InterruptedException {
        try ( Scanner consoleIn = new Scanner(System.in) ) {
            String topicName = args[0];
            String groupId = args[1];

            ConsumerThread consumer = new ConsumerThread(topicName, groupId);
            consumer.start();
            String line = "";
            while(! "exit".equals(line)) {
                line = consoleIn.next();
            }
            consumer.getKafkaConsumer().wakeup();
            System.out.println("Stopping consumer...");
            consumer.join();
        }
    }
}
