package com.kafkafka.cluster;

import java.util.*;

/**
 * Assigns partitions to brokers using round-robin.
 *
 * For topic "my-topic" with 3 partitions across brokers [1, 2, 3]:
 *   partition 0 → leader=1, replicas=[1, 2, 3]
 *   partition 1 → leader=2, replicas=[2, 3, 1]
 *   partition 2 → leader=3, replicas=[3, 1, 2]
 *
 * Replicas are spread so no two replicas of the same partition share a broker.
 */
public class PartitionAssigner {

    private final ClusterState clusterState;

    public PartitionAssigner(ClusterState clusterState) {
        this.clusterState = clusterState;
    }

    /**
     * Assigns partitions for a newly created topic.
     * Returns the list of PartitionMeta objects (also stored in ClusterState).
     */
    public List<ClusterState.PartitionMeta> assign(String topic, int numPartitions) {
        List<Integer> brokerIds = new ArrayList<>(clusterState.getLiveBrokers().keySet());
        Collections.sort(brokerIds);

        if (brokerIds.isEmpty()) {
            throw new IllegalStateException("No live brokers to assign partitions to");
        }

        int replicationFactor = Math.min(brokerIds.size(), 3); // up to 3 replicas
        List<ClusterState.PartitionMeta> result = new ArrayList<>();

        for (int p = 0; p < numPartitions; p++) {
            // Leader: round-robin across brokers
            int leaderIdx = p % brokerIds.size();
            int leaderId  = brokerIds.get(leaderIdx);

            // Replicas: leader first, then next N-1 brokers in order
            int[] replicaIds = new int[replicationFactor];
            for (int r = 0; r < replicationFactor; r++) {
                replicaIds[r] = brokerIds.get((leaderIdx + r) % brokerIds.size());
            }

            ClusterState.PartitionMeta meta =
                    new ClusterState.PartitionMeta(topic, p, leaderId, replicaIds);
            clusterState.setPartitionMeta(meta);
            result.add(meta);
        }

        return result;
    }

    /**
     * Re-assigns leadership when the current leader dies.
     * Picks the first ISR member that is still alive as the new leader.
     */
    public boolean electNewLeader(String topic, int partitionIndex) {
        ClusterState.PartitionMeta meta = clusterState.getPartitionMeta(topic, partitionIndex);
        if (meta == null) return false;

        for (int candidateId : meta.isrIds) {
            if (clusterState.isBrokerAlive(candidateId) && candidateId != meta.leaderId) {
                meta.leaderId = candidateId;
                System.out.println("New leader for " + topic + "-" + partitionIndex +
                                   ": broker " + candidateId);
                return true;
            }
        }
        System.err.println("No ISR member available for " + topic + "-" + partitionIndex);
        return false;
    }
}
