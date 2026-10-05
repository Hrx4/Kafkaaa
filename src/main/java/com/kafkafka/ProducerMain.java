package com.kafkafka;

import com.kafkafka.producer.Producer;
import java.util.Scanner;

public class ProducerMain {

    // All brokers in the cluster — try each one on failure
    private static final String[] BROKERS = {
            "localhost:9092",
            "localhost:9093",
            "localhost:9094"
    };

    private static Producer producer;
    private static int currentBrokerIndex = 0;

    public static void main(String[] args) throws Exception {
        connect();
        System.out.println("Type a message and press Enter. Type 'exit' to quit.\n");

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("> ");
            String line = scanner.nextLine().trim();
            if (line.equalsIgnoreCase("exit")) break;
            if (line.isEmpty()) continue;

            // Retry loop — if broker dies, reconnect to next one and resend
            boolean sent = false;
            for (int attempt = 0; attempt < BROKERS.length; attempt++) {
                try {
                    long[] result = producer.produce("my-topic", line, "");
                    System.out.printf("  ✓ sent to partition=%d, offset=%d (broker=%s)%n",
                            result[0], result[1], BROKERS[currentBrokerIndex]);
                    sent = true;
                    break;
                } catch (Exception e) {
                    System.out.println("  Broker " + BROKERS[currentBrokerIndex] +
                            " unreachable. Trying next broker...");
                    currentBrokerIndex = (currentBrokerIndex + 1) % BROKERS.length;
                    try { producer.close(); } catch (Exception ignored) {}
                    connect();
                }
            }
            if (!sent) {
                System.out.println("  ✗ All brokers unreachable. Message dropped.");
            }
        }

        producer.close();
        System.out.println("Producer closed.");
    }

    private static void connect() {
        for (int i = 0; i < BROKERS.length; i++) {
            String address = BROKERS[currentBrokerIndex];
            try {
                producer = Producer.newProducer(address);
                producer.connect();
                System.out.println("Connected to broker at " + address);
                return;
            } catch (Exception e) {
                System.out.println("Cannot connect to " + address + ". Trying next...");
                currentBrokerIndex = (currentBrokerIndex + 1) % BROKERS.length;
            }
        }
        System.out.println("Warning: No brokers available. Will retry on next message.");
    }
}