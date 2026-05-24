package com.kafkafka.partition;

import com.kafkafka.log.Log;
import com.kafkafka.log.Message;

import java.io.*;
import java.nio.file.*;
import java.util.List;

public class Partition {
    private final int id;
    private final String basePath;
    private final String topicName;
    private final Log log;

    public static Partition newPartition(int id, String basePath, String topicName) throws IOException {
        if (id < 1) throw new IllegalArgumentException("partition number cannot be negative");

        String topicDirPath = basePath + "/" + topicName;
        String paddedId     = String.format("p-%05d", id);
        String logDirPath   = topicDirPath + "/" + paddedId;

        Files.createDirectories(Paths.get(logDirPath));
        Log log = Log.newLog(logDirPath);
        return new Partition(id, basePath, topicName, log);
    }

    private Partition(int id, String basePath, String topicName, Log log) {
        this.id        = id;
        this.basePath  = basePath;
        this.topicName = topicName;
        this.log       = log;
    }

    public long appendMessage(String message) throws IOException {
        return log.append(message);
    }

    public List<Message> readMessages(long startOffset, int maxMessages) throws IOException {
        return log.bulkRead(startOffset, maxMessages);
    }

    public long getLatestOffset() {
        return log.getLatestOffset();
    }

    public void close() throws IOException {
        log.close();
    }
}
