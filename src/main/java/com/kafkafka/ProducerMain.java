package com.kafkafka;

import com.kafkafka.producer.Producer;
import java.util.Scanner;

public class ProducerMain {
    public static void main(String[] args) throws Exception {
        System.out.println("Connecting to broker...");
        Producer p = Producer.newProducer("localhost:9092");
        p.connect();
        System.out.println("Connected! Type a message and press Enter to send. Type 'exit' to quit.\n");

        Scanner scanner = new Scanner(System.in);
        while (true) {
            System.out.print("> ");
            String line = scanner.nextLine().trim();

            if (line.equalsIgnoreCase("exit")) break;
            if (line.isEmpty()) continue;

            long[] result = p.produce("my-topic", line, line); // use message as key
            System.out.printf("  ✓ sent to partition=%d, offset=%d%n", result[0], result[1]);
        }

        p.close();
        System.out.println("Producer closed.");
    }
}