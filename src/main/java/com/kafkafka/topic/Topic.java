package com.kafkafka.topic;

import com.kafkafka.log.Message;
import com.kafkafka.partition.Partition;
import com.kafkafka.utils.Utils;

import java.io.*;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;

public class Topic {
    private final String basePath;
    private final String topic;
    private final int numPartitions;
    private final List<Partition> partitions;

    public static Topic newTopic(String basePath, String topic, int numPartitions) throws IOException {
        if (numPartitions < 1) throw new IllegalArgumentException("number of partitions cannot be less than 1");
        if (topic == null || topic.isEmpty()) throw new IllegalArgumentException("topic name cannot be empty");
        Utils.validateDirPath(basePath);

        String topicDirPath = basePath + "/" + topic;
        Files.createDirectories(Paths.get(topicDirPath));

        List<String> existingPartitions = Utils.getDirectChildrenDirNames(topicDirPath);
        if (!existingPartitions.isEmpty() && existingPartitions.size() != numPartitions) {
            throw new IOException("conflict: topic exists with different number of partitions");
        }

        List<Partition> parts = new ArrayList<>();
        for (int i = 1; i <= numPartitions; i++) {
            parts.add(Partition.newPartition(i, basePath, topic));
        }

        return new Topic(basePath, topic, numPartitions, parts);
    }

    private Topic(String basePath, String topic, int numPartitions, List<Partition> partitions) {
        this.basePath      = basePath;
        this.topic         = topic;
        this.numPartitions = numPartitions;
        this.partitions    = partitions;
    }

    /**
     * Appends a message, returns [partitionNumber, offset].
     */
    public long[] append(String payload, String key) throws IOException {
        if (payload == null || payload.isEmpty())
            throw new IllegalArgumentException("you cannot write nothing lmao");

        int keyHash        = Utils.hash(key);
        int partitionIndex = keyHash % numPartitions;
        if (partitionIndex < 0) partitionIndex += numPartitions;

        Partition p = partitions.get(partitionIndex);
        long offset = p.appendMessage(payload);
        return new long[]{partitionIndex, offset};
    }

    public List<Message> read(int partitionNumber, long offset, int maxMessages) throws IOException {
        validatePartitionNumber(partitionNumber);
        return partitions.get(partitionNumber).readMessages(offset, maxMessages);
    }

    public long getLatestOffset(int partitionNumber) throws IOException {
        validatePartitionNumber(partitionNumber);
        return partitions.get(partitionNumber).getLatestOffset();
    }

    public int getNumberOfPartitions() {
        return numPartitions;
    }

    public void close() throws IOException {
        for (int i = 0; i < partitions.size(); i++) {
            try { partitions.get(i).close(); }
            catch (IOException e) { throw new IOException("error closing partition " + i + ": " + e.getMessage(), e); }
        }
    }

    private void validatePartitionNumber(int partitionNumber) {
        if (partitionNumber < 0 || partitionNumber >= numPartitions)
            throw new IllegalArgumentException("invalid partition number: " + partitionNumber);
    }
}
