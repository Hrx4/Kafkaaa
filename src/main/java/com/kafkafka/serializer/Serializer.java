package com.kafkafka.serializer;

import com.kafkafka.log.Message;
import com.kafkafka.types.Types;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * Wire serialization/deserialization – mirrors the Go serializer package exactly.
 * All strings are length-prefixed with a uint32 big-endian header.
 */
public class Serializer {

    private static final int MAX_STRING_LENGTH = 1 << 16;

    // ---- String helpers ----

    public static void serializeString(DataOutputStream out, String s) throws IOException {
        byte[] bytes = s != null ? s.getBytes() : new byte[0];
        if (bytes.length > MAX_STRING_LENGTH)
            throw new IOException("string too long: " + bytes.length);
        out.writeInt(bytes.length);
        if (bytes.length > 0) out.write(bytes);
    }

    public static String deserializeString(DataInputStream in) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > MAX_STRING_LENGTH)
            throw new IOException("invalid string length: " + length);
        if (length == 0) return "";
        byte[] buf = new byte[length];
        in.readFully(buf);
        return new String(buf);
    }

    // ---- Produce ----

    public static void serializeProduceRequest(DataOutputStream out, Types.ProduceRequest req) throws IOException {
        out.writeByte(Types.TYPE_PRODUCE);
        serializeString(out, req.topic);
        serializeString(out, req.payload);
        serializeString(out, req.key);
    }

    public static Types.ProduceRequest deserializeProduceRequest(DataInputStream in) throws IOException {
        String topic   = deserializeString(in);
        String payload = deserializeString(in);
        String key     = deserializeString(in);
        return new Types.ProduceRequest(topic, payload, key);
    }

    public static void serializeProduceResponse(DataOutputStream out, Types.ProduceResponse resp) throws IOException {
        out.writeInt(resp.partition);
        out.writeLong(resp.offset);
        serializeString(out, resp.error != null ? resp.error : "");
    }

    public static Types.ProduceResponse deserializeProduceResponse(DataInputStream in) throws IOException {
        int    partition = in.readInt();
        long   offset    = in.readLong();
        String error     = deserializeString(in);
        return new Types.ProduceResponse(partition, offset, error);
    }

    // ---- Consume ----

    public static void serializeConsumeRequest(DataOutputStream out, Types.ConsumeRequest req) throws IOException {
        out.writeByte(Types.TYPE_CONSUME);
        serializeString(out, req.topic);
        out.writeInt(req.partition);
        out.writeLong(req.offset);
        out.writeShort(req.max);
    }

    public static Types.ConsumeRequest deserializeConsumeRequest(DataInputStream in) throws IOException {
        String topic     = deserializeString(in);
        int    partition = in.readInt();
        long   offset    = in.readLong();
        short  max       = in.readShort();
        return new Types.ConsumeRequest(topic, partition, offset, max);
    }

    public static void serializeConsumeResponse(DataOutputStream out, Types.ConsumeResponse resp) throws IOException {
        serializeString(out, resp.error != null ? resp.error : "");
        out.writeInt(resp.messages != null ? resp.messages.size() : 0);
        if (resp.messages != null) {
            for (Message msg : resp.messages) {
                out.writeLong(msg.offset);
                out.writeLong(msg.timestamp);
                serializeString(out, msg.payload != null ? new String(msg.payload) : "");
            }
        }
    }

    public static Types.ConsumeResponse deserializeConsumeResponse(DataInputStream in) throws IOException {
        String error       = deserializeString(in);
        int    numMessages = in.readInt();
        List<Message> messages = new ArrayList<>(numMessages);
        for (int i = 0; i < numMessages; i++) {
            long   offset    = in.readLong();
            long   timestamp = in.readLong();
            String payload   = deserializeString(in);
            messages.add(new Message(offset, timestamp, payload.getBytes()));
        }
        return new Types.ConsumeResponse(messages, error);
    }

    // ---- CreateTopic ----

    public static void serializeCreateTopicRequest(DataOutputStream out, Types.CreateTopicRequest req) throws IOException {
        out.writeByte(Types.TYPE_CREATE_TOPIC);
        serializeString(out, req.topic);
        out.writeShort(req.numPartitions);
    }

    public static Types.CreateTopicRequest deserializeCreateTopicRequest(DataInputStream in) throws IOException {
        String topic         = deserializeString(in);
        short  numPartitions = in.readShort();
        return new Types.CreateTopicRequest(topic, numPartitions);
    }

    public static void serializeCreateTopicResponse(DataOutputStream out, Types.CreateTopicResponse resp) throws IOException {
        serializeString(out, resp.error != null ? resp.error : "");
    }

    public static Types.CreateTopicResponse deserializeCreateTopicResponse(DataInputStream in) throws IOException {
        return new Types.CreateTopicResponse(deserializeString(in));
    }

    // ---- ListTopics ----

    public static void serializeListTopicsRequest(DataOutputStream out) throws IOException {
        out.writeByte(Types.TYPE_LIST_TOPICS);
    }

    public static void serializeListTopicsResponse(DataOutputStream out, Types.ListTopicsResponse resp) throws IOException {
        serializeString(out, resp.error != null ? resp.error : "");
        out.writeInt(resp.topics != null ? resp.topics.size() : 0);
        if (resp.topics != null) {
            for (Types.TopicInfo t : resp.topics) {
                serializeString(out, t.name);
                out.writeShort(t.numPartitions);
            }
        }
    }

    public static Types.ListTopicsResponse deserializeListTopicsResponse(DataInputStream in) throws IOException {
        String error     = deserializeString(in);
        int    numTopics = in.readInt();
        List<Types.TopicInfo> topics = new ArrayList<>(numTopics);
        for (int i = 0; i < numTopics; i++) {
            String name          = deserializeString(in);
            short  numPartitions = in.readShort();
            topics.add(new Types.TopicInfo(name, numPartitions));
        }
        return new Types.ListTopicsResponse(topics, error);
    }
}
