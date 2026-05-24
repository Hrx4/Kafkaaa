package com.kafkafka;

import com.kafkafka.broker.Broker;
import com.kafkafka.consumer.Consumer;
import com.kafkafka.log.Message;
import com.kafkafka.producer.Producer;
import com.kafkafka.types.Types;

import java.util.List;

/**
 * Demo main – mirrors the behaviour of cmd/main.go
 */
public class Main {

    public static void main(String[] args) throws Exception {
        final String TOPIC_NAME = "batching-test";
        final String BROKER_ADDR = "localhost:9092";

        // --- Ensure data directory exists ---
        new java.io.File("kafka-data").mkdirs();

        // --- Start broker in background thread ---
        Broker broker = Broker.newBroker(BROKER_ADDR, "kafka-data");
        Thread brokerThread = new Thread(() -> {
            try { broker.start(); }
            catch (Exception e) { System.err.println("broker error: " + e.getMessage()); }
        });
        brokerThread.setDaemon(true);
        brokerThread.start();

        Thread.sleep(100); // wait for broker to bind
        System.out.println(broker.status());

        // --- Create topic ---
        broker.createTopic(TOPIC_NAME, (short) 1);

        // --- Producer ---
        Producer p = Producer.newProducer(BROKER_ADDR);
        p.connect();

        for (int i = 1; i <= 3; i++) {
            long[] result = p.produce(TOPIC_NAME, "message-" + i, "");
            System.out.printf("Produced message %d to partition %d, offset %d%n", i, result[0], result[1]);
        }
        p.close();

        Thread.sleep(1000); // wait for batch flush

        // --- Consumer ---
        Consumer c = Consumer.newConsumer(BROKER_ADDR);
        c.connect();
        System.out.println("consumer status: " + c.status());

        List<Types.TopicInfo> topics = c.listTopics();
        System.out.println("Available topics: " + topics);

        List<Message> messages = c.consume(TOPIC_NAME, 0, 1, (short) 10);
        for (int i = 0; i < messages.size(); i++) {
            Message msg = messages.get(i);
            System.out.printf("Consumed message %d: Payload=%s, Offset=%d%n",
                    i + 1, new String(msg.payload), msg.offset);
        }

        c.close();

        // --- Stop broker ---
        broker.stop();
        System.out.println(broker.status());
    }
}