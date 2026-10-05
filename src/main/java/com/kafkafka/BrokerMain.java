package com.kafkafka;

import com.kafkafka.broker.Broker;
import com.kafkafka.config.BrokerConfig;
import java.io.File;

/**
 * Single broker:
 *   mvn compile exec:java "-Dexec.mainClass=com.kafkafka.BrokerMain"
 *
 * Cluster (3 terminals):
 *   mvn exec:java "-Dexec.mainClass=com.kafkafka.BrokerMain" "-Dexec.args=broker1.properties"
 *   mvn exec:java "-Dexec.mainClass=com.kafkafka.BrokerMain" "-Dexec.args=broker2.properties"
 *   mvn exec:java "-Dexec.mainClass=com.kafkafka.BrokerMain" "-Dexec.args=broker3.properties"
 */
public class BrokerMain {
    public static void main(String[] args) throws Exception {

        BrokerConfig config;
        if (args.length > 0) {
            config = BrokerConfig.load(args[0]);
            System.out.println("Loaded: " + config);
        } else {
            config = BrokerConfig.of(1, "localhost:9092", "kafka-data",
                    new String[]{"localhost:9092"});
        }

        new File(config.dataDir).mkdirs();

        Broker broker = Broker.newBroker(config);

        // Start assignment receiver BEFORE start() so follower brokers
        // can receive partition assignments from the controller
        broker.startAssignmentReceiver();

        // Create topic — controller will assign + broadcast to followers
        broker.createTopic("my-topic", (short) 3);

        System.out.println("==============================");
        System.out.println("  Broker " + config.brokerId + " on " + config.address);
        System.out.println("  Press Ctrl+C to stop");
        System.out.println("==============================");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { broker.stop(); System.out.println("Broker stopped."); }
            catch (Exception ignored) {}
        }));

        broker.start(); // blocks — starts TCP + heartbeat + election inside
    }
}