package com.kafkafka.cluster;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Shared in-memory cluster state.
 * The controller writes it; all brokers read it to route requests.
 *
 * Partition key format: "topicName-partitionIndex"  e.g. "my-topic-0"
 */
public class ClusterState {

    public static class BrokerInfo {
        public final int    brokerId;
        public final String address;
        public volatile long lastHeartbeatMs;

        public BrokerInfo(int brokerId, String address) {
            this.brokerId        = brokerId;
            this.address         = address;
            this.lastHeartbeatMs = System.currentTimeMillis();
        }
    }

    public static class PartitionMeta {
        public final String topic;
        public final int    partitionIndex;
        public volatile int leaderId;
        public final int[]  replicaIds;   // all replicas (ordered)
        public volatile int[] isrIds;     // in-sync replicas

        public PartitionMeta(String topic, int partitionIndex, int leaderId, int[] replicaIds) {
            this.topic          = topic;
            this.partitionIndex = partitionIndex;
            this.leaderId       = leaderId;
            this.replicaIds     = replicaIds;
            this.isrIds         = replicaIds.clone();
        }

        public String key() {
            return topic + "-" + partitionIndex;
        }
    }

    // ---- State ----
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private final Map<Integer, BrokerInfo>   liveBrokers = new ConcurrentHashMap<>();
    private final Map<String, PartitionMeta> partitions  = new ConcurrentHashMap<>();
    private volatile int controllerBrokerId = -1;

    // ---- Broker registration ----

    public void registerBroker(int brokerId, String address) {
        liveBrokers.put(brokerId, new BrokerInfo(brokerId, address));
    }

    public void updateHeartbeat(int brokerId) {
        BrokerInfo info = liveBrokers.get(brokerId);
        if (info != null) info.lastHeartbeatMs = System.currentTimeMillis();
    }

    public void removeBroker(int brokerId) {
        liveBrokers.remove(brokerId);
    }

    public Map<Integer, BrokerInfo> getLiveBrokers() {
        return Collections.unmodifiableMap(liveBrokers);
    }

    public BrokerInfo getBroker(int brokerId) {
        return liveBrokers.get(brokerId);
    }

    public boolean isBrokerAlive(int brokerId) {
        return liveBrokers.containsKey(brokerId);
    }

    // ---- Partition metadata ----

    public void setPartitionMeta(PartitionMeta meta) {
        partitions.put(meta.key(), meta);
    }

    public PartitionMeta getPartitionMeta(String topic, int partitionIndex) {
        return partitions.get(topic + "-" + partitionIndex);
    }

    public Map<String, PartitionMeta> getAllPartitions() {
        return Collections.unmodifiableMap(partitions);
    }

    /** Returns the address of the leader broker for this partition, or null if unknown. */
    public String getLeaderAddress(String topic, int partitionIndex) {
        PartitionMeta meta = getPartitionMeta(topic, partitionIndex);
        if (meta == null) return null;
        BrokerInfo leader = liveBrokers.get(meta.leaderId);
        return leader != null ? leader.address : null;
    }

    // ---- Controller ----

    public void setController(int brokerId) {
        this.controllerBrokerId = brokerId;
    }

    public int getControllerBrokerId() {
        return controllerBrokerId;
    }

    public boolean isController(int brokerId) {
        return controllerBrokerId == brokerId;
    }
}
