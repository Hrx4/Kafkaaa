package com.kafkafka.producer;

import com.kafkafka.serializer.Serializer;
import com.kafkafka.types.Types;
import com.kafkafka.utils.Utils;

import java.io.*;
import java.net.Socket;

public class Producer {

    private final String address;
    private Socket socket;
    private DataInputStream  in;
    private DataOutputStream out;
    private boolean isConnected = false;

    public static Producer newProducer(String address) throws IOException {
        Utils.validateTCPAddress(address);
        return new Producer(address);
    }

    private Producer(String address) {
        this.address = address;
    }

    public void connect() throws IOException {
        String[] parts = address.split(":");
        socket = new Socket(parts[0], Integer.parseInt(parts[1]));
        in     = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        out    = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
        isConnected = true;
    }

    /**
     * Returns [partitionNumber, offset].
     */
    public long[] produce(String topic, String payload, String key) throws IOException {
        if (topic == null || topic.isEmpty() || payload == null || payload.isEmpty())
            throw new IllegalArgumentException("empty string topic or payload");
        if (!isConnected)
            throw new IllegalStateException("producer not connected");

        Types.ProduceRequest req = new Types.ProduceRequest(topic, payload, key != null ? key : "");
        Serializer.serializeProduceRequest(out, req);
        out.flush();

        Types.ProduceResponse resp = Serializer.deserializeProduceResponse(in);
        if (resp.error != null && !resp.error.isEmpty())
            throw new IOException(resp.error);

        return new long[]{resp.partition, resp.offset};
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
