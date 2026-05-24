package com.kafkafka;

import com.kafkafka.broker.Broker;
import java.io.File;

public class BrokerMain {
    public static void main(String[] args) throws Exception {
        new File("kafka-data").mkdirs();
        Broker broker = Broker.newBroker("localhost:9092", "kafka-data");

        // Create a topic on startup
        broker.createTopic("my-topic", (short) 3);

        System.out.println("Broker started: " + broker.status());
        broker.start(); // blocks here, accepting connections
    }
}