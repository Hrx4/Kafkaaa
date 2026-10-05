package com.kafkafka.heartbeat;

import com.kafkafka.cluster.ClusterState;
import com.kafkafka.cluster.ControllerElection;

import java.io.*;
import java.net.*;
import java.util.concurrent.*;
import java.util.logging.Logger;

/**
 * Sends a heartbeat to every other broker in the cluster every second.
 * Format (plain text over TCP): "HB:<brokerId>:<address>\n"
 *
 * Also registers this broker in ClusterState and triggers re-election
 * when a peer stops responding.
 */
public class HeartbeatSender {

    private static final Logger log = Logger.getLogger(HeartbeatSender.class.getName());

    private static final int HEARTBEAT_INTERVAL_MS = 1000;
    private static final int HEARTBEAT_TIMEOUT_MS  = 3000; // broker dead after 3s no heartbeat

    private final int               localBrokerId;
    private final String            localAddress;
    private final String[]          peerAddresses; // all OTHER brokers
    private final ClusterState      clusterState;
    private final ControllerElection election;

    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "heartbeat-sender");
                t.setDaemon(true);
                return t;
            });

    public HeartbeatSender(int localBrokerId, String localAddress,
                           String[] allBrokerAddresses,
                           ClusterState clusterState,
                           ControllerElection election) {
        this.localBrokerId = localBrokerId;
        this.localAddress  = localAddress;
        this.clusterState  = clusterState;
        this.election      = election;

        // Filter out our own address
        this.peerAddresses = java.util.Arrays.stream(allBrokerAddresses)
                .filter(a -> !a.equals(localAddress))
                .toArray(String[]::new);

        // Register ourselves immediately
        clusterState.registerBroker(localBrokerId, localAddress);
    }

    public void start() {
        scheduler.scheduleAtFixedRate(this::sendHeartbeats,
                0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        log.info("HeartbeatSender started for broker " + localBrokerId);
    }

    public void stop() {
        scheduler.shutdownNow();
    }

    private void sendHeartbeats() {
        for (String peerAddress : peerAddresses) {
            try {
                sendHeartbeatTo(peerAddress);
            } catch (Exception e) {
                // Peer unreachable — mark it dead and re-elect if needed
                handlePeerUnreachable(peerAddress, e.getMessage());
            }
        }
        // Also check existing brokers for timeout
        checkForDeadBrokers();
    }

    private void sendHeartbeatTo(String address) throws IOException {
        String[] parts = address.split(":");
        String host = parts[0];
        int    port  = Integer.parseInt(parts[1]) + 100; // heartbeat port = broker port + 100

        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 500); // 500ms connect timeout
            socket.setSoTimeout(500);
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
            writer.println("HB:" + localBrokerId + ":" + localAddress);

            // Read ACK
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(socket.getInputStream()));
            String ack = reader.readLine();
            if (ack != null && ack.startsWith("ACK:")) {
                int peerId = Integer.parseInt(ack.split(":")[1]);
                clusterState.updateHeartbeat(peerId);
            }
        }
    }

    private void handlePeerUnreachable(String address, String reason) {
        // Find broker ID by address and remove if timed out
        clusterState.getLiveBrokers().forEach((id, info) -> {
            if (info.address.equals(address)) {
                long silentMs = System.currentTimeMillis() - info.lastHeartbeatMs;
                if (silentMs > HEARTBEAT_TIMEOUT_MS) {
                    log.warning("Broker " + id + " at " + address +
                                " silent for " + silentMs + "ms — marking dead");
                    clusterState.removeBroker(id);
                    election.reElect();
                }
            }
        });
    }

    private void checkForDeadBrokers() {
        long now = System.currentTimeMillis();
        clusterState.getLiveBrokers().forEach((id, info) -> {
            if (id == localBrokerId) return; // skip self
            long silentMs = now - info.lastHeartbeatMs;
            if (silentMs > HEARTBEAT_TIMEOUT_MS) {
                log.warning("Broker " + id + " timed out (" + silentMs + "ms). Removing.");
                clusterState.removeBroker(id);
                election.reElect();
            }
        });
    }
}
