package com.kafkafka.replication;

import com.kafkafka.consumer.Consumer;
import com.kafkafka.log.Message;
import com.kafkafka.partition.Partition;

import java.util.List;
import java.util.logging.Logger;

/**
 * Runs on a FOLLOWER broker.
 * Continuously fetches new messages from the leader and appends them to the local log.
 *
 * Uses the existing Consumer client to talk to the leader broker —
 * this reuses the same TCP protocol without adding a new request type.
 */
public class ReplicationFetcher implements Runnable {

    private static final Logger log = Logger.getLogger(ReplicationFetcher.class.getName());
    private static final int FETCH_INTERVAL_MS = 50;  // poll every 50ms
    private static final int MAX_FETCH          = 100; // messages per fetch

    private final String    leaderAddress;
    private final String    topic;
    private final int       partitionIndex;
    private final int       localBrokerId;
    private final Partition localPartition;    // our local copy of this partition
    private final ISRManager isrManager;

    private volatile boolean running = false;
    private Consumer consumer;

    public ReplicationFetcher(String leaderAddress, String topic, int partitionIndex,
                               int localBrokerId, Partition localPartition,
                               ISRManager isrManager) {
        this.leaderAddress  = leaderAddress;
        this.topic          = topic;
        this.partitionIndex = partitionIndex;
        this.localBrokerId  = localBrokerId;
        this.localPartition = localPartition;
        this.isrManager     = isrManager;
    }

    public void start() {
        running = true;
        Thread t = new Thread(this, "replication-fetcher-" + topic + "-" + partitionIndex);
        t.setDaemon(true);
        t.start();
    }

    public void stop() {
        running = false;
        if (consumer != null) {
            try { consumer.close(); } catch (Exception ignored) {}
        }
    }

    @Override
    public void run() {
        // Connect to leader
        while (running) {
            try {
                consumer = Consumer.newConsumer(leaderAddress);
                consumer.connect();
                log.info("ReplicationFetcher connected to leader at " + leaderAddress +
                         " for " + topic + "-" + partitionIndex);
                break;
            } catch (Exception e) {
                log.warning("Cannot connect to leader at " + leaderAddress + ". Retrying in 2s...");
                sleep(2000);
            }
        }

        // Fetch loop
        long fetchOffset = localPartition.getLatestOffset() + 1; // start after what we already have
        if (fetchOffset < 1) fetchOffset = 1;

        while (running) {
            try {
                List<Message> messages = consumer.consume(
                        topic, partitionIndex, fetchOffset, (short) MAX_FETCH);

                for (Message msg : messages) {
                    // Append to local log — reuse the payload as-is
                    localPartition.appendMessage(new String(msg.payload));
                    fetchOffset = msg.offset + 1;

                    // Report progress to ISR manager
                    isrManager.updateReplicaProgress(topic, partitionIndex,
                            localBrokerId, msg.offset);
                }
            } catch (Exception e) {
                String err = e.getMessage();
                if (err != null &&
                    (err.contains("not found in index") || err.contains("offset out of range"))) {
                    // Nothing new yet — normal, just wait
                } else {
                    log.warning("Fetch error from leader " + leaderAddress + ": " + err);
                    sleep(1000); // back off on real errors
                }
            }
            sleep(FETCH_INTERVAL_MS);
        }
    }

    private void sleep(long ms) {
        try { Thread.sleep(ms); }
        catch (InterruptedException e) { Thread.currentThread().interrupt(); }
    }
}
