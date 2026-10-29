package com.kafkafka.broker;

import com.kafkafka.topic.Topic;
import com.kafkafka.types.Types;
import com.kafkafka.utils.Utils;

import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class Broker {

    private final String dataDir;
    private final String address;
    private final Map<String, Topic> topics;
    private final ReentrantReadWriteLock mutex = new ReentrantReadWriteLock();
    private ServerSocket serverSocket;
    private volatile boolean isRunning = false;
    private ExecutorService threadPool;

    public static Broker newBroker(String address, String dataDir) throws IOException {
        Utils.validateTCPAddress(address);
        Utils.validateDirPath(dataDir);

        Map<String, Topic> topics = new ConcurrentHashMap<>();
        List<String> topicNames = Utils.getDirectChildrenDirNames(dataDir);
//        System.out.println("topicNames : " + topicNames);
        for (String topicName : topicNames) {
            List<String> partitionNames = Utils.getDirectChildrenDirNames(dataDir + "/" + topicName);
            System.out.println( "partitionNames : " + partitionNames);
            Topic topic = Topic.newTopic(dataDir, topicName, partitionNames.size());

            topics.put(topicName, topic);
        }

        return new Broker(address, dataDir, topics);
    }

    private Broker(String address, String dataDir, Map<String, Topic> topics) {
        this.address  = address;
        this.dataDir  = dataDir;
        this.topics   = topics;
    }

    public void start() throws IOException {
        String[] parts = address.split(":");
        int port = Integer.parseInt(parts[1]);

        serverSocket = new ServerSocket(port);
        isRunning    = true;
        threadPool   = Executors.newCachedThreadPool();

        System.out.println("listening on address: " + address);
        while (isRunning) {
            try {
                Socket conn = serverSocket.accept();
                threadPool.submit(() -> handleConnection(conn));
            } catch (SocketException e) {
                if (!isRunning) break; // normal shutdown
                throw e;
            }
        }
    }

    public void stop() throws IOException {
        isRunning = false;
        if (serverSocket != null) serverSocket.close();
        if (threadPool   != null) threadPool.shutdown();

        mutex.writeLock().lock();
        try {
            for (Map.Entry<String, Topic> e : topics.entrySet()) {
                e.getValue().close();
            }
        } finally {
            mutex.writeLock().unlock();
        }
    }

    public String status() {
        return isRunning ? "RUNNING" : "STOPPED";
    }

    public void createTopic(String topicName, short numPartitions) throws IOException {
        mutex.readLock().lock();
        boolean exists = topics.containsKey(topicName);
        mutex.readLock().unlock();
        if (exists) return;

        Topic topic = Topic.newTopic(dataDir, topicName, numPartitions);
        mutex.writeLock().lock();
        try { topics.put(topicName, topic); }
        finally { mutex.writeLock().unlock(); }
    }

    public List<Types.TopicInfo> listTopics() {
        List<Types.TopicInfo> list = new ArrayList<>();
        mutex.readLock().lock();
        try {
            for (Map.Entry<String, Topic> e : topics.entrySet()) {
                list.add(new Types.TopicInfo(e.getKey(), (short) e.getValue().getNumberOfPartitions()));
            }
        } finally {
            mutex.readLock().unlock();
        }
        return list;
    }

    // ---- Connection handling ----

    private void handleConnection(Socket conn) {
        try (conn) {
            BrokerHandler handler = new BrokerHandler(conn, this);
            handler.handle();
        } catch (IOException e) {
            // client disconnected or error
        }
    }

    // ---- Request handlers (called by BrokerHandler) ----

    Types.ProduceResponse handleProduce(Types.ProduceRequest req) {
        mutex.readLock().lock();
        Topic topic = topics.get(req.topic);
        mutex.readLock().unlock();

        if (topic == null)
            return new Types.ProduceResponse(0, 0, "topic not found");

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

        if (topic == null)
            return new Types.ConsumeResponse(null, "topic not found");

        try {
            var messages = topic.read(req.partition, req.offset, req.max);
            return new Types.ConsumeResponse(messages, "");
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
