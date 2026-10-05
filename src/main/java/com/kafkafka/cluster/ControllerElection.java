package com.kafkafka.cluster;

import java.util.*;
import java.util.logging.Logger;

/**
 * Simple controller election: the broker with the lowest ID
 * among all live brokers is the controller.
 *
 * Real Kafka uses ZooKeeper / KRaft for this.
 * We use the simpler "lowest ID" rule which works fine for a local cluster.
 */
public class ControllerElection {

    private static final Logger log = Logger.getLogger(ControllerElection.class.getName());

    private final ClusterState clusterState;
    private final int          localBrokerId;

    public ControllerElection(ClusterState clusterState, int localBrokerId) {
        this.clusterState  = clusterState;
        this.localBrokerId = localBrokerId;
    }

    /**
     * Runs an election based on current live brokers.
     * Returns the elected controller broker ID.
     */
    public int elect() {
        List<Integer> liveBrokerIds = new ArrayList<>(clusterState.getLiveBrokers().keySet());
        if (liveBrokerIds.isEmpty()) {
            // Only us
            clusterState.setController(localBrokerId);
            log.info("No other brokers alive. Broker " + localBrokerId + " is the controller.");
            return localBrokerId;
        }

        int elected = Collections.min(liveBrokerIds);
        clusterState.setController(elected);
        log.info("Controller elected: broker " + elected +
                 " (candidates: " + liveBrokerIds + ")");
        return elected;
    }

    /**
     * Called when a broker joins or leaves. Re-runs election and returns new controller ID.
     */
    public int reElect() {
        int newController = elect();
        if (newController == localBrokerId) {
            log.info("This broker (" + localBrokerId + ") is now the controller.");
        } else {
            log.info("Controller is broker " + newController + " (this broker: " + localBrokerId + ")");
        }
        return newController;
    }

    public boolean isLocalBrokerController() {
        return clusterState.isController(localBrokerId);
    }
}
