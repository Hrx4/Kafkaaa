package com.kafkafka.consumer;

import com.kafkafka.log.Message;
import com.kafkafka.serializer.Serializer;
import com.kafkafka.types.Types;
import com.kafkafka.utils.Utils;

import java.io.*;
import java.net.Socket;
import java.util.List;

public class Consumer {

    private final String address;
    private Socket socket;
    private DataInputStream  in;
    private DataOutputStream out;
    private boolean isConnected = false;

    public static Consumer newConsumer(String address) throws IOException {
        Utils.validateTCPAddress(address);
        return new Consumer(address);
    }

    private Consumer(String address) {
        this.address = address;
    }

    public void connect() throws IOException {
        String[] parts = address.split(":");
        socket = new Socket(parts[0], Integer.parseInt(parts[1]));
        in     = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out    = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        isConnected = true;
    }

    public List<Message> consume(String topic, int partition, long offset, short maxMessages) throws IOException {
        if (topic == null || topic.isEmpty())
            throw new IllegalArgumentException("topic cannot be empty string");
        if (partition < 0 || offset < 0 || maxMessages < 1)
            throw new IllegalArgumentException("invalid parameter");
        if (!isConnected)
            throw new IllegalStateException("consumer not connected");

        Types.ConsumeRequest req = new Types.ConsumeRequest(topic, partition, offset, maxMessages);
        Serializer.serializeConsumeRequest(out, req);
        out.flush();

        Types.ConsumeResponse resp = Serializer.deserializeConsumeResponse(in);
        if (resp.error != null && !resp.error.isEmpty())
            throw new IOException(resp.error);

        return resp.messages;
    }

    public List<Types.TopicInfo> listTopics() throws IOException {
        if (!isConnected) throw new IllegalStateException("consumer not connected");

        Serializer.serializeListTopicsRequest(out);
        out.flush();

        Types.ListTopicsResponse resp = Serializer.deserializeListTopicsResponse(in);
        if (resp.error != null && !resp.error.isEmpty())
            throw new IOException(resp.error);

        return resp.topics;
    }

    public void close() throws IOException {
        if (!isConnected) return;
        socket.close();
        isConnected = false;
    }

    public String status() {
        return isConnected ? "CONNECTED" : "DISCONNECTED";
    }
}
