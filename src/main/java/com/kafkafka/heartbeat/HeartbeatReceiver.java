package com.kafkafka.heartbeat;

import com.kafkafka.cluster.ClusterState;
import com.kafkafka.cluster.ControllerElection;

import java.io.*;
import java.net.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Logger;

/**
 * Listens on (brokerPort + 100) for heartbeat messages from peer brokers.
 * When a new broker appears, it registers it in ClusterState and re-elects.
 */
public class HeartbeatReceiver {

    private static final Logger log = Logger.getLogger(HeartbeatReceiver.class.getName());

    private final int               heartbeatPort;
    private final int               localBrokerId;
    private final ClusterState      clusterState;
    private final ControllerElection election;

    private ServerSocket     serverSocket;
    private volatile boolean running = false;
    private final ExecutorService pool = Executors.newCachedThreadPool(r -> {
        Thread t = new Thread(r, "heartbeat-receiver");
        t.setDaemon(true);
        return t;
    });

    public HeartbeatReceiver(int brokerPort, int localBrokerId,
                             ClusterState clusterState,
                             ControllerElection election) {
        this.heartbeatPort  = brokerPort + 100; // heartbeat on broker port + 100
        this.localBrokerId  = localBrokerId;
        this.clusterState   = clusterState;
        this.election       = election;
    }

    public void start() throws IOException {
        serverSocket = new ServerSocket(heartbeatPort);
        running = true;
        pool.submit(() -> {
            log.info("HeartbeatReceiver listening on port " + heartbeatPort);
            while (running) {
                try {
                    Socket conn = serverSocket.accept();
                    pool.submit(() -> handleHeartbeat(conn));
                } catch (SocketException e) {
                    if (!running) break;
                } catch (IOException e) {
                    log.warning("HeartbeatReceiver accept error: " + e.getMessage());
                }
            }
        });
    }

    public void stop() {
        running = false;
        pool.shutdownNow();
        try { if (serverSocket != null) serverSocket.close(); }
        catch (IOException ignored) {}
    }

    private void handleHeartbeat(Socket conn) {
        try (conn) {
            conn.setSoTimeout(1000);
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            PrintWriter writer = new PrintWriter(conn.getOutputStream(), true);

            String line = reader.readLine();
            if (line == null || !line.startsWith("HB:")) return;

            // Format: "HB:<brokerId>:<address>"
            String[] parts   = line.split(":");
            int    peerId    = Integer.parseInt(parts[1]);
            String peerAddr  = parts[2] + ":" + parts[3]; // host:port

            boolean isNew = !clusterState.isBrokerAlive(peerId);
            if (isNew) {
                clusterState.registerBroker(peerId, peerAddr);
                log.info("New broker registered: id=" + peerId + " addr=" + peerAddr);
                election.reElect();
            } else {
                clusterState.updateHeartbeat(peerId);
            }

            // Send ACK
            writer.println("ACK:" + localBrokerId);
        } catch (IOException e) {
            // connection dropped — ignore
        }
    }
}
