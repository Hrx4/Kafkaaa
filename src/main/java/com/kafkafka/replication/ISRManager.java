package com.kafkafka.replication;

import com.kafkafka.cluster.ClusterState;

import java.util.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * Tracks which follower replicas are "in sync" with the leader.
 *
 * A replica is in-sync if it has fetched up to within ISR_LAG_MAX_MS
 * of the leader's latest offset. If it falls behind, it's removed from ISR.
 * When it catches up, it's added back.
 */
public class ISRManager {

    private static final Logger log = Logger.getLogger(ISRManager.class.getName());
    private static final long ISR_LAG_MAX_MS = 10_000; // 10 seconds

    public static class ReplicaProgress {
        public final int    brokerId;
        public volatile long fetchedOffset;      // last offset this replica fetched
        public volatile long lastFetchTimeMs;

        public ReplicaProgress(int brokerId) {
            this.brokerId       = brokerId;
            this.fetchedOffset  = 0;
            this.lastFetchTimeMs = System.currentTimeMillis();
        }
    }

    // key: "topic-partitionIndex"
    private final Map<String, Map<Integer, ReplicaProgress>> progress = new ConcurrentHashMap<>();
    private final ClusterState clusterState;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "isr-manager");
                t.setDaemon(true);
                return t;
            });

    public ISRManager(ClusterState clusterState) {
        this.clusterState = clusterState;
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::checkISR, 1, 1, TimeUnit.SECONDS);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    /** Called by the replication fetcher on the follower side after each fetch. */
    public void updateReplicaProgress(String topic, int partitionIndex,
                                      int brokerId, long fetchedOffset) {
        String key = topic + "-" + partitionIndex;
        progress.computeIfAbsent(key, k -> new ConcurrentHashMap<>())
                .compute(brokerId, (id, existing) -> {
                    if (existing == null) existing = new ReplicaProgress(brokerId);
                    existing.fetchedOffset   = fetchedOffset;
                    existing.lastFetchTimeMs = System.currentTimeMillis();
                    return existing;
                });
    }

    /** Returns the minimum offset that ALL ISR replicas have confirmed — safe to ack to producer. */
    public long getHighWatermark(String topic, int partitionIndex, long leaderOffset) {
        String key = topic + "-" + partitionIndex;
        Map<Integer, ReplicaProgress> partitionProgress = progress.get(key);
        if (partitionProgress == null || partitionProgress.isEmpty()) return leaderOffset;

        ClusterState.PartitionMeta meta = clusterState.getPartitionMeta(topic, partitionIndex);
        if (meta == null) return leaderOffset;

        long min = leaderOffset;
        for (int isrId : meta.isrIds) {
            ReplicaProgress rp = partitionProgress.get(isrId);
            if (rp != null && rp.fetchedOffset < min) {
                min = rp.fetchedOffset;
            }
        }
        return min;
    }

    /** Periodically checks replicas and updates ISR lists. */
    private void checkISR() {
        long now = System.currentTimeMillis();

        clusterState.getAllPartitions().forEach((key, meta) -> {
            Map<Integer, ReplicaProgress> partProgress = progress.get(key);
            if (partProgress == null) return;

            List<Integer> newISR = new ArrayList<>();
            for (int replicaId : meta.replicaIds) {
                if (replicaId == meta.leaderId) {
                    newISR.add(replicaId); // leader always in ISR
                    continue;
                }
                ReplicaProgress rp = partProgress.get(replicaId);
                if (rp == null) continue;

                long lagMs = now - rp.lastFetchTimeMs;
                if (lagMs <= ISR_LAG_MAX_MS) {
                    newISR.add(replicaId);
                } else {
                    log.warning("Replica " + replicaId + " lagging " + lagMs +
                                "ms on " + key + " — removing from ISR");
                }
            }
            meta.isrIds = newISR.stream().mapToInt(Integer::intValue).toArray();
        });
    }

    public Map<Integer, ReplicaProgress> getProgress(String topic, int partitionIndex) {
        return progress.getOrDefault(topic + "-" + partitionIndex, Collections.emptyMap());
    }
}
