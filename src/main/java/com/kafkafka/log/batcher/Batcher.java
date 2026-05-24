package com.kafkafka.log.batcher;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;

public class Batcher {

    // --- Config constants ---
    public static final int DEFAULT_MAX_MESSAGE_SIZE_BYTES = 16 * 1024; // 16 KB
    public static final int DEFAULT_MESSAGES_PER_BATCH = 32;
    public static final int DEFAULT_FLUSH_INTERVAL_MS = 100;

    // --- Inner types ---
    public static class ChannelInput {
        public final byte[] logInput;
        public final int offset;

        public ChannelInput(byte[] logInput, int offset) {
            this.logInput = logInput;
            this.offset = offset;
        }
    }

    private static class BatchMetaEntry {
        int offset;
        int size;

        BatchMetaEntry(int offset, int size) {
            this.offset = offset;
            this.size = size;
        }
    }

    // --- Fields ---
    private final BatcherSideEffects sideEffects;
    public final BlockingQueue<ChannelInput> ch;
    private final int flushIntervalMs;
    private final int flushSizeBytes;
    private final ByteArrayOutputStream buffer;
    private final List<BatchMetaEntry> batchMeta;
    private final RandomAccessFile logFile;
    private final RandomAccessFile indexFile;
    private volatile boolean running = false;
    private Thread batcherThread;

    public Batcher(BatcherSideEffects sideEffects,
                   int messagesPerBatch, int maxMessageSizeBytes, int flushIntervalMs,
                   String logFilePath, String indexFilePath) throws IOException {
        this.sideEffects = sideEffects;
        this.flushIntervalMs = flushIntervalMs;
        this.flushSizeBytes = messagesPerBatch * maxMessageSizeBytes;
        int channelCapacity = 2 * messagesPerBatch;

        this.ch = new LinkedBlockingQueue<>(channelCapacity);
        this.buffer = new ByteArrayOutputStream(2 * flushSizeBytes);
        this.batchMeta = new ArrayList<>();

        // Open files in append mode
        this.logFile = new RandomAccessFile(logFilePath, "rw");
        this.logFile.seek(this.logFile.length()); // seek to end for appending
        this.indexFile = new RandomAccessFile(indexFilePath, "rw");
        this.indexFile.seek(this.indexFile.length());
    }

    private void flushAndEmpty() {
        if (buffer.size() == 0) return;

        try {
            // Current file pointer is already at end (we always append),
            // so this is the byte offset where our batch will land.
            long firstWriteOffset = logFile.getFilePointer();

            byte[] data = buffer.toByteArray();
            logFile.write(data);
            logFile.getFD().sync();

            StringBuilder indexEntries = new StringBuilder();
            int aggregatedSizes = 0;
            List<int[]> offsetsSlice = new ArrayList<>();

            for (BatchMetaEntry meta : batchMeta) {
                int writeOffset = (int) firstWriteOffset + aggregatedSizes;
                indexEntries.append(meta.offset).append(":").append(writeOffset).append(",");
                aggregatedSizes += meta.size;
                offsetsSlice.add(new int[]{meta.offset, writeOffset});
            }

            sideEffects.onBatchDone(offsetsSlice);

            indexFile.writeBytes(indexEntries.toString());
            indexFile.getFD().sync();
        } catch (IOException e) {
            // log error; in production would surface this properly
            System.err.println("Batcher flush error: " + e.getMessage());
        }

        empty();
    }

    private void empty() {
        buffer.reset();
        batchMeta.clear();
    }

    /** Main batcher loop – call in a dedicated thread. */
    public void start() {
        running = true;
        batcherThread = new Thread(() -> {
            long lastFlushTime = System.currentTimeMillis();
            while (running) {
                try {
                    long now = System.currentTimeMillis();
                    long waitMs = flushIntervalMs - (now - lastFlushTime);
                    if (waitMs <= 0) {
                        if (buffer.size() > 0) flushAndEmpty();
                        lastFlushTime = System.currentTimeMillis();
                        continue;
                    }

                    ChannelInput input = ch.poll(waitMs, TimeUnit.MILLISECONDS);
                    if (input != null) {
                        // ByteArrayOutputStream.write(byte[]) never actually throws IOException,
                        // but the compiler requires the catch because the signature declares it.
                        try {
                            buffer.write(input.logInput);
                        } catch (IOException ignored) { /* never thrown by ByteArrayOutputStream */ }
                        batchMeta.add(new BatchMetaEntry(input.offset, input.logInput.length));
                        if (buffer.size() >= flushSizeBytes) {
                            flushAndEmpty();
                            lastFlushTime = System.currentTimeMillis();
                        }
                    } else {
                        // timeout elapsed
                        if (buffer.size() > 0) flushAndEmpty();
                        lastFlushTime = System.currentTimeMillis();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
            // Flush remaining on stop
            if (buffer.size() > 0) flushAndEmpty();
        }, "batcher-thread");
        batcherThread.setDaemon(true);
        batcherThread.start();
    }

    public void stop() {
        running = false;
        if (batcherThread != null) {
            batcherThread.interrupt();
            try { batcherThread.join(2000); } catch (InterruptedException ignored) {}
        }
        try { logFile.close(); } catch (IOException ignored) {}
        try { indexFile.close(); } catch (IOException ignored) {}
    }
}