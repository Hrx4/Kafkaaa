package com.kafkafka;

import com.kafkafka.consumer.Consumer;
import com.kafkafka.log.Message;

import java.io.*;
import java.nio.file.*;
import java.util.List;

public class ConsumerMain {

    private static final String TOPIC      = "my-topic";
    private static final int    PARTITIONS = 3;
    private static final int    POLL_MS    = 200;
    private static final String OFFSET_DIR = "consumer-offsets"; // saved offset files go here

    public static void main(String[] args) throws Exception {

        // Make sure offset directory exists
        new File(OFFSET_DIR).mkdirs();

        System.out.println("Connecting to broker...");

        Consumer[] consumers = new Consumer[PARTITIONS];
        long[]     offsets   = new long[PARTITIONS];

        for (int i = 0; i < PARTITIONS; i++) {
            consumers[i] = Consumer.newConsumer("localhost:9092");
            consumers[i].connect();
            offsets[i] = loadOffset(i);   // <-- resume from saved offset, not always 1
            System.out.printf("  partition %d → resuming from offset %d%n", i, offsets[i]);
        }

        System.out.println("\nListening for messages on '" + TOPIC + "'...");
        System.out.println("(Ctrl+C to stop — offsets are saved automatically)\n");

        // Save offsets on shutdown (Ctrl+C)
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nShutting down, saving offsets...");
            for (int i = 0; i < PARTITIONS; i++) {
                saveOffset(i, offsets[i]);
                System.out.printf("  partition %d → saved offset %d%n", i, offsets[i]);
            }
            for (Consumer c : consumers) {
                try { c.close(); } catch (Exception ignored) {}
            }
        }));

        // Poll loop
        while (true) {
            for (int partition = 0; partition < PARTITIONS; partition++) {
                try {
                    List<Message> messages = consumers[partition].consume(
                            TOPIC, partition, offsets[partition], (short) 10);

                    for (Message msg : messages) {
                        System.out.printf("[partition=%d | offset=%d] %s%n",
                                partition, msg.offset, new String(msg.payload));
                        offsets[partition] = msg.offset + 1; // advance in-memory offset
                        saveOffset(partition, offsets[partition]); // persist immediately
                    }
                } catch (Exception e) {
                    String err = e.getMessage();
                    if (err == null) continue;
                    // these just mean no new messages yet — not real errors
                    if (err.contains("offset out of range") ||
                            err.contains("not found in index")) continue;
                    System.err.println("Error on partition " + partition + ": " + err);
                }
            }
            Thread.sleep(POLL_MS);
        }
    }

    // --- Offset persistence helpers ---

    private static long loadOffset(int partition) {
        Path file = offsetFile(partition);
        if (!Files.exists(file)) return 1L; // no saved offset → start from beginning
        try {
            String content = Files.readString(file).trim();
            return Long.parseLong(content);
        } catch (Exception e) {
            return 1L;
        }
    }

    private static void saveOffset(int partition, long offset) {
        try {
            Files.writeString(offsetFile(partition), String.valueOf(offset));
        } catch (IOException e) {
            System.err.println("Failed to save offset for partition " + partition + ": " + e.getMessage());
        }
    }

    private static Path offsetFile(int partition) {
        return Paths.get(OFFSET_DIR, "partition-" + partition + ".offset");
    }
}