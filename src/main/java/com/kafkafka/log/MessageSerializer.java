package com.kafkafka.log;

import java.io.*;
import java.nio.ByteBuffer;

/**
 * Binary format:
 * [length (4 bytes)][offset (8 bytes)][timestamp (8 bytes)][payload length (4 bytes)][payload]
 */
public class MessageSerializer {

    public static byte[] serialize(long offset, byte[] payload) throws IOException {
        // Build content (without length prefix)
        ByteArrayOutputStream contentBuf = new ByteArrayOutputStream();
        DataOutputStream dos = new DataOutputStream(contentBuf);
        dos.writeLong(offset);
        dos.writeLong(System.currentTimeMillis());
        dos.writeInt(payload != null ? payload.length : 0);
        if (payload != null && payload.length > 0) {
            dos.write(payload);
        }
        dos.flush();

        byte[] content = contentBuf.toByteArray();
        int length = content.length;

        // Build final: [length prefix][content]
        ByteArrayOutputStream finalBuf = new ByteArrayOutputStream();
        DataOutputStream finalDos = new DataOutputStream(finalBuf);
        finalDos.writeInt(length);
        finalDos.write(content);
        finalDos.flush();
        return finalBuf.toByteArray();
    }

    public static Message deserialize(byte[] data) throws IOException {
        if (data.length < 24) { // 4 (length) + 8 (offset) + 8 (timestamp) + 4 (payload length)
            throw new IOException("data too short: got " + data.length + " bytes, need at least 24");
        }

        DataInputStream dis = new DataInputStream(new ByteArrayInputStream(data));

        int length = dis.readInt();
        if (length != data.length - 4) {
            throw new IOException("invalid length: got " + (data.length - 4) + ", expected " + length);
        }
        if (length < 20) {
            throw new IOException("invalid message length: " + length);
        }

        long offset = dis.readLong();
        long timestamp = dis.readLong();
        int payloadLen = dis.readInt();
        if (payloadLen < 0) {
            throw new IOException("invalid payload length: " + payloadLen);
        }
        if (dis.available() < payloadLen) {
            throw new IOException("incomplete payload: got " + dis.available() + " bytes, need " + payloadLen);
        }

        byte[] payload = new byte[payloadLen];
        if (payloadLen > 0) {
            dis.readFully(payload);
        }

        if (dis.available() > 0) {
            throw new IOException("extra bytes in data: " + dis.available() + " remaining");
        }

        return new Message(offset, timestamp, payload);
    }
}
