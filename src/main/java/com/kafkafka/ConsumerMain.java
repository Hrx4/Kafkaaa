package com.kafkafka;

import com.kafkafka.consumer.Consumer;
import com.kafkafka.log.Message;

import java.io.*;
import java.nio.file.*;
import java.util.List;

public class ConsumerMain {

    private static final String[] BROKERS    = {
            "localhost:9092",
            "localhost:9093",
            "localhost:9094"
    };
    private static final String TOPIC      = "my-topic";
    private static final int    PARTITION  = 0;
    private static final int    POLL_MS    = 200;
    private static final String OFFSET_DIR = "consumer-offsets";

    private static Consumer consumer;
    private static int currentBrokerIndex = 0;

    public static void main(String[] args) throws Exception {
        new File(OFFSET_DIR).mkdirs();
        connect();

        long[] offset = { loadOffset(PARTITION) };
        System.out.printf("Resuming from offset %d on partition %d%n", offset[0], PARTITION);
        System.out.println("Waiting for messages...\n");

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            saveOffset(PARTITION, offset[0]);
            System.out.printf("%nOffset %d saved. Bye.%n", offset[0]);
            try { consumer.close(); } catch (Exception ignored) {}
        }));

        while (true) {
            try {
                List<Message> messages = consumer.consume(
                        TOPIC, PARTITION, offset[0], (short) 10);

                for (Message msg : messages) {
                    System.out.printf("[offset=%-4d] %s%n",
                            msg.offset, new String(msg.payload));
                    offset[0] = msg.offset + 1;
                    saveOffset(PARTITION, offset[0]);
                }
            } catch (Exception e) {
                String err = e.getMessage();
                if (err == null || err.contains("not found in index") ||
                        err.contains("offset out of range")) {
                    // no new messages — normal
                } else {
                    // broker died — reconnect to next one
                    System.out.println("Broker unreachable. Reconnecting...");
                    try { consumer.close(); } catch (Exception ignored) {}
                    currentBrokerIndex = (currentBrokerIndex + 1) % BROKERS.length;
                    connect();
                }
            }
            Thread.sleep(POLL_MS);
        }
    }

    private static void connect() {
        for (int i = 0; i < BROKERS.length; i++) {
            String address = BROKERS[currentBrokerIndex];
            try {
                consumer = Consumer.newConsumer(address);
                consumer.connect();
                System.out.println("Connected to broker at " + address);
                return;
            } catch (Exception e) {
                System.out.println("Cannot connect to " + address + ". Trying next...");
                currentBrokerIndex = (currentBrokerIndex + 1) % BROKERS.length;
            }
        }
        System.out.println("Warning: No brokers available. Will retry on next poll.");
    }

    private static long loadOffset(int partition) {
        Path file = offsetFile(partition);
        if (!Files.exists(file)) return 1L;
        try { return Long.parseLong(Files.readString(file).trim()); }
        catch (Exception e) { return 1L; }
    }

    private static void saveOffset(int partition, long offset) {
        try { Files.writeString(offsetFile(partition), String.valueOf(offset)); }
        catch (IOException e) { System.err.println("Failed to save offset: " + e.getMessage()); }
    }

    private static Path offsetFile(int partition) {
        return Paths.get(OFFSET_DIR, "partition-" + partition + ".offset");
    }
}