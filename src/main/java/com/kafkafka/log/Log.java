package com.kafkafka.log;

import com.kafkafka.log.batcher.Batcher;
import com.kafkafka.log.batcher.BatcherSideEffects;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class Log implements BatcherSideEffects {

    private final String dirPath;
    private final ReentrantLock mutex = new ReentrantLock();
    private final Map<Long, Long> index = new HashMap<>();
    private volatile long offset = 0;           // last written logical offset
    private volatile long lastIndexedOffset = 0; // last flushed-to-disk offset
    private RandomAccessFile logFile;
    private RandomAccessFile indexFile;
    private Batcher batcher;

    // ---- BatcherSideEffects ----
    @Override
    public void onBatchDone(List<int[]> offsets) {
        mutex.lock();
        try {
            int max = 0;
            for (int[] entry : offsets) {
                int logicalOffset = entry[0];
                int writeOffset   = entry[1];
                index.put((long) logicalOffset, (long) writeOffset);
                if (logicalOffset > max) max = logicalOffset;
            }
            lastIndexedOffset = max;
        } finally {
            mutex.unlock();
        }
    }

    // ---- Construction ----

    public static Log newLog(String dirPath) throws IOException {
        Path dir = Paths.get(dirPath);
        if (!Files.isDirectory(dir)) {
            throw new IOException("not a directory: " + dirPath);
        }

        String logFilePath   = dirPath + "/.log";
        String indexFilePath = dirPath + "/.index";

        // Create files if they don't exist
        new File(logFilePath).createNewFile();
        new File(indexFilePath).createNewFile();

        Log log = new Log(dirPath, logFilePath, indexFilePath);

        // Load existing index
        long lastOffset = log.loadIndexAndGetLastOffset();
        log.offset = lastOffset;
        log.lastIndexedOffset = lastOffset;

        // Validate; reconstruct if invalid
        try {
            log.validateIndex();
        } catch (IOException e) {
            log.reconstructIndexFromLog();
        }

        // Start batcher
        log.batcher.start();
        return log;
    }

    private Log(String dirPath, String logFilePath, String indexFilePath) throws IOException {
        this.dirPath = dirPath;
        this.logFile   = new RandomAccessFile(logFilePath, "rw");
        this.indexFile = new RandomAccessFile(indexFilePath, "rw");

        this.batcher = new Batcher(
                this,
                Batcher.DEFAULT_MESSAGES_PER_BATCH,
                Batcher.DEFAULT_MAX_MESSAGE_SIZE_BYTES,
                Batcher.DEFAULT_FLUSH_INTERVAL_MS,
                logFilePath,
                indexFilePath
        );
    }

    // ---- Index loading / persistence ----

    private long loadIndexAndGetLastOffset() {
        mutex.lock();
        try {
            indexFile.seek(0);
            byte[] data = new byte[(int) indexFile.length()];
            indexFile.readFully(data);
            String str = new String(data);
            if (str.isEmpty()) return 0;

            String entries = str.endsWith(",") ? str.substring(0, str.length() - 1) : str;
            long lastOffset = 0;
            for (String entry : entries.split(",")) {
                String[] kv = entry.split(":");
                if (kv.length == 2) {
                    try {
                        long key   = Long.parseLong(kv[0].trim());
                        long value = Long.parseLong(kv[1].trim());
                        if (key > lastOffset) lastOffset = key;
                        index.put(key, value);
                    } catch (NumberFormatException ignored) {}
                }
            }
            return lastOffset;
        } catch (IOException e) {
            return 0;
        } finally {
            mutex.unlock();
        }
    }

    private void validateIndex() throws IOException {
        mutex.lock();
        try {
            if (offset == 0) return; // nothing written yet

            Long pos = index.get(offset);
            if (pos == null) throw new IOException("last offset not found in index");

            logFile.seek(pos);
            int msgLen = logFile.readInt();

            byte[] offsetBuf = new byte[8];
            logFile.readFully(offsetBuf);
            long storedOffset = ByteBuffer.wrap(offsetBuf).getLong();
            if (storedOffset != offset) throw new IOException("index mismatch");

            logFile.seek(pos + 4 + msgLen);
            // try reading one more byte – should be EOF
            int b = logFile.read();
            if (b != -1) throw new IOException("invalid index: more messages found after lastOffset");
        } finally {
            mutex.unlock();
        }
    }

    private void reconstructIndexFromLog() throws IOException {
        mutex.lock();
        try {
            index.clear();
            logFile.seek(0);

            long logicalOffset = 1;
            long fileOffset    = 0;
            long lastOffset    = 0;

            while (true) {
                byte[] lenBuf = new byte[4];
                int n = logFile.read(lenBuf);
                if (n <= 0) break;
                if (n != 4) break;

                long length = Integer.toUnsignedLong(ByteBuffer.wrap(lenBuf).getInt());
                index.put(logicalOffset, fileOffset);
                lastOffset = logicalOffset;
                logicalOffset++;
                logFile.seek(logFile.getFilePointer() + length);
                fileOffset += 4 + length;
            }

            this.offset = lastOffset;
            persistIndex();
        } finally {
            mutex.unlock();
        }
    }

    private void persistIndex() throws IOException {
        StringBuilder sb = new StringBuilder();
        for (Map.Entry<Long, Long> e : index.entrySet()) {
            sb.append(e.getKey()).append(":").append(e.getValue()).append(",");
        }
        indexFile.seek(0);
        indexFile.writeBytes(sb.toString());
        indexFile.getFD().sync();
    }

    // ---- Read ----

    private Message readAt(long fileOffset) throws IOException {
        mutex.lock();
        try {
            logFile.seek(fileOffset);
            int msgLength = logFile.readInt();

            logFile.seek(fileOffset);
            byte[] buf = new byte[4 + msgLength];
            logFile.readFully(buf);
            return MessageSerializer.deserialize(buf);
        } finally {
            mutex.unlock();
        }
    }

    public Message read(long offset) throws IOException {
        Long writeOffset = index.get(offset);
        if (writeOffset == null) throw new IOException("offset " + offset + " not found in index");
        return readAt(writeOffset);
    }

    private List<Long> getOffsetsRangeSlice(long startOffset, int max) {
        List<Long> result = new ArrayList<>();
        long cur = startOffset;
        while (cur <= lastIndexedOffset) {
            if (index.containsKey(cur)) {
                result.add(cur);
                if (result.size() == max) break;
            }
            cur++;
        }
        return result;
    }

    public List<Message> bulkRead(long startOffset, int max) throws IOException {
        List<Message> messages = new ArrayList<>();

        long maxReadable = max;
        if (startOffset + max - 1 > lastIndexedOffset) {
            maxReadable = lastIndexedOffset - startOffset + 1;
        }
        if (maxReadable <= 0) return messages;

        if (maxReadable == 1) {
            messages.add(read(startOffset));
            return messages;
        }

        mutex.lock();
        try {
            List<Long> offsets = getOffsetsRangeSlice(startOffset, (int) maxReadable);
            if (offsets.isEmpty()) return messages;

            long minWriteOffset = index.get(offsets.get(0));
            long maxWriteOffset = minWriteOffset;
            for (long o : offsets) {
                long wo = index.get(o);
                if (wo < minWriteOffset) minWriteOffset = wo;
                if (wo > maxWriteOffset) maxWriteOffset = wo;
            }

            // Read size of the last message
            logFile.seek(maxWriteOffset);
            int lastMsgLen = logFile.readInt();
            long size = maxWriteOffset + 4 + lastMsgLen - minWriteOffset;

            logFile.seek(minWriteOffset);
            byte[] buffer = new byte[(int) size];
            logFile.readFully(buffer);

            for (long msgOffset : offsets) {
                long msgWriteOffset = index.get(msgOffset);
                int bufferOffset = (int)(msgWriteOffset - minWriteOffset);

                int msgLength = ByteBuffer.wrap(buffer, bufferOffset, 4).getInt();
                int totalSize = 4 + msgLength;
                byte[] msgBytes = Arrays.copyOfRange(buffer, bufferOffset, bufferOffset + totalSize);
                messages.add(MessageSerializer.deserialize(msgBytes));
            }
        } finally {
            mutex.unlock();
        }

        return messages;
    }

    // ---- Write ----

    public long append(String message) throws IOException {
        long newOffset;
        mutex.lock();
        try {
            newOffset = offset + 1;
            offset = newOffset;
        } finally {
            mutex.unlock();
        }

        byte[] msgBytes = MessageSerializer.serialize(newOffset, message.getBytes());
        Batcher.ChannelInput input = new Batcher.ChannelInput(msgBytes, (int) newOffset);

        try {
            batcher.ch.put(input); // blocking if full – matches Go channel semantics
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("interrupted while queuing message", e);
        }

        return newOffset;
    }

    public long getLatestOffset() {
        return lastIndexedOffset;
    }

    public void close() throws IOException {
        mutex.lock();
        try {
            batcher.stop();
            logFile.close();
            indexFile.close();
        } finally {
            mutex.unlock();
        }
    }
}
