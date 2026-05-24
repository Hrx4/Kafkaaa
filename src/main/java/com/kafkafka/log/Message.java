package com.kafkafka.log;

public class Message {
    public long offset;
    public long timestamp;
    public byte[] payload;

    public Message(long offset, long timestamp, byte[] payload) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.payload = payload;
    }

    public void print() {
        System.out.printf("Offset: %d, Timestamp: %d, Payload: %s%n",
                offset, timestamp, new String(payload));
    }
}
