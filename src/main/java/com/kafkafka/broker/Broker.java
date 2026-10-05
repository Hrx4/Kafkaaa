package com.kafkafka.broker;

import com.kafkafka.cluster.ClusterState;
import com.kafkafka.cluster.ControllerElection;
import com.kafkafka.cluster.PartitionAssigner;
import com.kafkafka.config.BrokerConfig;
import com.kafkafka.heartbeat.HeartbeatReceiver;
import com.kafkafka.heartbeat.HeartbeatSender;
import com.kafkafka.partition.Partition;
import com.kafkafka.replication.ISRManager;
import com.kafkafka.replication.ReplicationFetcher;
import com.kafkafka.topic.Topic;
import com.kafkafka.types.Types;
import com.kafkafka.utils.Utils;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Broker {

    private final BrokerConfig         config;
    private final String               dataDir;
    private final String               address;
    private final int                  brokerId;

    public  final ClusterState         clusterState;
    private final ControllerElection   election;
    private final PartitionAssigner    partitionAssigner;
    private final ISRManager           isrManager;
    private       HeartbeatSender      heartbeatSender;
    private       HeartbeatReceiver    heartbeatReceiver;

    private final List<ReplicationFetcher>  fetchers   = new CopyOnWriteArrayList<>();
    private final Map<String, Topic>        topics     = new ConcurrentHashMap<>();
    private final ReentrantReadWriteLock    mutex      = new ReentrantReadWriteLock();

    private ServerSocket     serverSocket;
    private volatile boolean isRunning = false;
    private ExecutorService  threadPool;

    // =========================================================
    // Factory
    // =========================================================

    public static Broker newBroker(String address, String dataDir) throws IOException {
        BrokerConfig config = BrokerConfig.of(1, address, dataDir, new String[]{address});
        return newBroker(config);
    }

    public static Broker newBroker(BrokerConfig config) throws IOException {
        Utils.validateTCPAddress(config.address);
        new File(config.dataDir).mkdirs();
        Utils.validateDirPath(config.dataDir);

        ClusterState      clusterState      = new ClusterState();
        ControllerElection election         = new ControllerElection(clusterState, config.brokerId);
        PartitionAssigner partitionAssigner = new PartitionAssigner(clusterState);
        ISRManager        isrManager        = new ISRManager(clusterState);

        Map<String, Topic> topics = new ConcurrentHashMap<>();
        List<String> topicNames = Utils.getDirectChildrenDirNames(config.dataDir);
        for (String topicName : topicNames) {
            List<String> partitionDirs =
                    Utils.getDirectChildrenDirNames(config.dataDir + "/" + topicName);
            if (!partitionDirs.isEmpty()) {
                Topic topic = Topic.newTopic(config.dataDir, topicName, partitionDirs.size());
                topics.put(topicName, topic);
            }
        }

        return new Broker(config, clusterState, election, partitionAssigner, isrManager, topics);
    }

    private Broker(BrokerConfig config, ClusterState clusterState,
                   ControllerElection election, PartitionAssigner partitionAssigner,
                   ISRManager isrManager, Map<String, Topic> initialTopics) {
        this.config           = config;
        this.address          = config.address;
        this.dataDir          = config.dataDir;
        this.brokerId         = config.brokerId;
        this.clusterState     = clusterState;
        this.election         = election;
        this.partitionAssigner = partitionAssigner;
        this.isrManager       = isrManager;
        this.topics.putAll(initialTopics);
    }

    // =========================================================
    // Lifecycle
    // =========================================================

    public void start() throws IOException {
        clusterState.registerBroker(brokerId, address);
        isrManager.start();

        int port = Integer.parseInt(address.split(":")[1]);

        if (config.clusterBrokers.length > 1) {
            // Start heartbeat receiver first so peers can reach us
            heartbeatReceiver = new HeartbeatReceiver(port, brokerId, clusterState, election);
            heartbeatReceiver.start();

            heartbeatSender = new HeartbeatSender(
                    brokerId, address, config.clusterBrokers, clusterState, election);
            heartbeatSender.start();

            // Wait for peers to exchange heartbeats and register in ClusterState
            System.out.println("Broker " + brokerId + " waiting for peers...");
            try { Thread.sleep(2000); } catch (InterruptedException ignored) {}
        }

        // Elect controller after peers are known
        election.elect();

        boolean isController = election.isLocalBrokerController();
        System.out.println("Broker " + brokerId + " listening on " + address +
                (isController ? " [CONTROLLER]" : " [FOLLOWER]") +
                " | live brokers: " + clusterState.getLiveBrokers().keySet());

        // TCP server
        serverSocket = new ServerSocket(port);
        isRunning    = true;
        threadPool   = Executors.newCachedThreadPool();

        while (isRunning) {
            try {
                Socket conn = serverSocket.accept();
                threadPool.submit(() -> handleConnection(conn));
            } catch (SocketException e) {
                if (!isRunning) break;
                throw e;
            }
        }
    }

    public void stop() throws IOException {
        isRunning = false;
        if (serverSocket      != null) serverSocket.close();
        if (threadPool        != null) threadPool.shutdown();
        if (heartbeatSender   != null) heartbeatSender.stop();
        if (heartbeatReceiver != null) heartbeatReceiver.stop();
        isrManager.stop();
        fetchers.forEach(ReplicationFetcher::stop);
        mutex.writeLock().lock();
        try { topics.values().forEach(t -> { try { t.close(); } catch (IOException ignored) {} }); }
        finally { mutex.writeLock().unlock(); }
    }

    public String status()     { return isRunning ? "RUNNING" : "STOPPED"; }
    public int    getBrokerId(){ return brokerId; }

    // =========================================================
    // Topic management
    // =========================================================

    public void createTopic(String topicName, short numPartitions) throws IOException {
        mutex.readLock().lock();
        boolean exists = topics.containsKey(topicName);
        mutex.readLock().unlock();
        if (exists) return;

        Topic topic = Topic.newTopic(dataDir, topicName, numPartitions);
        mutex.writeLock().lock();
        try { topics.put(topicName, topic); }
        finally { mutex.writeLock().unlock(); }

        if (config.clusterBrokers.length > 1) {
            if (election.isLocalBrokerController()) {
                // Controller: assign partitions and broadcast assignment
                List<ClusterState.PartitionMeta> assignments =
                        partitionAssigner.assign(topicName, numPartitions);

                System.out.println("Partition assignments for '" + topicName + "':");
                for (ClusterState.PartitionMeta m : assignments) {
                    System.out.printf("  partition %d → leader=broker%d replicas=%s%n",
                            m.partitionIndex, m.leaderId, Arrays.toString(m.replicaIds));
                }

                // Push assignment to all follower brokers via their assignment endpoint
                broadcastAssignment(topicName, assignments);

                // Start our own fetchers for partitions where we are a follower
                startFetchersForAssignments(topicName, assignments);
            }
            // Followers: wait for assignment to arrive via broadcast
            // Their fetchers are started inside receiveAssignment()
        }
    }

    /**
     * Called by the controller to push partition assignments to follower brokers.
     * Uses a simple TCP message: "ASSIGN:<topic>:<partition>:<leaderId>:<replica1>,<replica2>,...\n"
     */
    private void broadcastAssignment(String topicName,
                                     List<ClusterState.PartitionMeta> assignments) {
        clusterState.getLiveBrokers().forEach((peerId, peerInfo) -> {
            if (peerId == brokerId) return; // skip self
            try {
                String[] parts = peerInfo.address.split(":");
                // Assignment port = broker port + 200
                int assignPort = Integer.parseInt(parts[1]) + 200;
                try (Socket sock = new Socket()) {
                    sock.connect(new InetSocketAddress(parts[0], assignPort), 1000);
                    PrintWriter w = new PrintWriter(sock.getOutputStream(), true);
                    for (ClusterState.PartitionMeta m : assignments) {
                        String replicaStr = Arrays.stream(m.replicaIds)
                                .mapToObj(String::valueOf)
                                .reduce((a, b) -> a + "," + b).orElse("");
                        w.println("ASSIGN:" + topicName + ":" + m.partitionIndex +
                                ":" + m.leaderId + ":" + replicaStr);
                    }
                    w.println("DONE");
                }
                System.out.println("  Sent assignments to broker" + peerId);
            } catch (Exception e) {
                System.err.println("  Failed to send assignment to broker" + peerId +
                        ": " + e.getMessage());
            }
        });
    }

    /**
     * Starts the assignment receiver server on port (brokerPort + 200).
     * Called from HeartbeatReceiver after cluster is up — but we start it in start() here.
     */
    public void startAssignmentReceiver() throws IOException {
        int port = Integer.parseInt(address.split(":")[1]) + 200;
        ServerSocket assignServer = new ServerSocket(port);
        Thread t = new Thread(() -> {
            System.out.println("Assignment receiver listening on port " + port);
            while (isRunning) {
                try {
                    Socket conn = assignServer.accept();
                    threadPool.submit(() -> receiveAssignment(conn));
                } catch (Exception e) {
                    if (!isRunning) break;
                }
            }
            try { assignServer.close(); } catch (IOException ignored) {}
        }, "assignment-receiver");
        t.setDaemon(true);
        t.start();
    }

    private void receiveAssignment(Socket conn) {
        try (conn) {
            BufferedReader reader = new BufferedReader(
                    new InputStreamReader(conn.getInputStream()));
            List<ClusterState.PartitionMeta> received = new ArrayList<>();
            String topicName = null;

            String line;
            while ((line = reader.readLine()) != null) {
                if (line.equals("DONE")) break;
                if (!line.startsWith("ASSIGN:")) continue;

                // ASSIGN:<topic>:<partition>:<leaderId>:<replicas>
                String[] parts     = line.split(":");
                topicName          = parts[1];
                int partitionIndex = Integer.parseInt(parts[2]);
                int leaderId       = Integer.parseInt(parts[3]);
                int[] replicaIds   = Arrays.stream(parts[4].split(","))
                        .mapToInt(Integer::parseInt).toArray();

                ClusterState.PartitionMeta meta =
                        new ClusterState.PartitionMeta(topicName, partitionIndex,
                                leaderId, replicaIds);
                clusterState.setPartitionMeta(meta);
                received.add(meta);
                System.out.printf("  [broker%d] Received assignment: %s partition %d → leader=broker%d%n",
                        brokerId, topicName, partitionIndex, leaderId);
            }

            if (topicName == null || received.isEmpty()) return;

            // Make sure topic exists locally
            if (!topics.containsKey(topicName)) {
                short numPartitions = (short) received.size();
                Topic topic = Topic.newTopic(dataDir, topicName, numPartitions);
                mutex.writeLock().lock();
                try { topics.put(topicName, topic); }
                finally { mutex.writeLock().unlock(); }
            }

            // Start fetchers for partitions where we are a follower
            startFetchersForAssignments(topicName, received);

        } catch (Exception e) {
            System.err.println("Error receiving assignment: " + e.getMessage());
        }
    }

    private void startFetchersForAssignments(String topicName,
                                             List<ClusterState.PartitionMeta> assignments) {
        for (ClusterState.PartitionMeta meta : assignments) {
            // Skip if we are the leader for this partition
            if (meta.leaderId == brokerId) continue;

            // Check if we are a replica
            boolean isReplica = false;
            for (int rid : meta.replicaIds) {
                if (rid == brokerId) { isReplica = true; break; }
            }
            if (!isReplica) continue;

            // Get leader address
            ClusterState.BrokerInfo leaderInfo = clusterState.getBroker(meta.leaderId);
            if (leaderInfo == null) {
                System.err.println("Leader broker" + meta.leaderId + " not in ClusterState yet");
                continue;
            }

            try {
                Partition localPartition = Partition.newPartition(
                        meta.partitionIndex + 1, dataDir, topicName);
                ReplicationFetcher fetcher = new ReplicationFetcher(
                        leaderInfo.address, topicName, meta.partitionIndex,
                        brokerId, localPartition, isrManager);
                fetcher.start();
                fetchers.add(fetcher);
                System.out.printf("  [broker%d] Replication fetcher started: " +
                                "%s-%d from leader broker%d (%s)%n",
                        brokerId, topicName, meta.partitionIndex,
                        meta.leaderId, leaderInfo.address);
            } catch (IOException e) {
                System.err.println("Failed to start fetcher for " + topicName +
                        "-" + meta.partitionIndex + ": " + e.getMessage());
            }
        }
    }

    public List<Types.TopicInfo> listTopics() {
        List<Types.TopicInfo> list = new ArrayList<>();
        mutex.readLock().lock();
        try {
            topics.forEach((name, t) ->
                    list.add(new Types.TopicInfo(name, (short) t.getNumberOfPartitions())));
        } finally { mutex.readLock().unlock(); }
        return list;
    }

    // =========================================================
    // Request handlers
    // =========================================================

    private void handleConnection(Socket conn) {
        try (conn) { new BrokerHandler(conn, this).handle(); }
        catch (IOException ignored) {}
    }

    Types.ProduceResponse handleProduce(Types.ProduceRequest req) {
        mutex.readLock().lock();
        Topic topic = topics.get(req.topic);
        mutex.readLock().unlock();
        if (topic == null) return new Types.ProduceResponse(0, 0, "topic not found");
        try {
            long[] result = topic.append(req.payload, req.key);
            return new Types.ProduceResponse((int) result[0], result[1], "");
        } catch (IOException e) {
            return new Types.ProduceResponse(0, 0, e.getMessage());
        }
    }

    Types.ConsumeResponse handleConsume(Types.ConsumeRequest req) {
        mutex.readLock().lock();
        Topic topic = topics.get(req.topic);
        mutex.readLock().unlock();
        if (topic == null) return new Types.ConsumeResponse(null, "topic not found");
        try {
            return new Types.ConsumeResponse(topic.read(req.partition, req.offset, req.max), "");
        } catch (IOException e) {
            return new Types.ConsumeResponse(null, e.getMessage());
        }
    }

    Types.CreateTopicResponse handleCreateTopic(Types.CreateTopicRequest req) {
        try {
            createTopic(req.topic, req.numPartitions);
            return new Types.CreateTopicResponse("");
        } catch (IOException e) {
            return new Types.CreateTopicResponse(e.getMessage());
        }
    }

    Types.ListTopicsResponse handleListTopics() {
        return new Types.ListTopicsResponse(listTopics(), "");
    }
}