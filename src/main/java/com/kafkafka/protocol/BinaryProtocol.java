package com.kafkafka.protocol;

import com.kafkafka.serializer.Serializer;
import com.kafkafka.types.Types;

import java.io.*;
import java.net.Socket;

/**
 * Abstracts serialization from the broker handlers.
 * Reads a 1-byte type code then delegates to the correct deserializer.
 */
public class BinaryProtocol {

    private final DataInputStream  in;
    private final DataOutputStream out;

    public BinaryProtocol(Socket socket) throws IOException {
        this.in  = new DataInputStream(new BufferedInputStream(socket.getInputStream()));
        this.out = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream()));
    }

    public Types.Request readRequest() throws IOException {
        int reqType = in.readUnsignedByte();
        switch (reqType) {
            case Types.TYPE_PRODUCE:      return Serializer.deserializeProduceRequest(in);
            case Types.TYPE_CONSUME:      return Serializer.deserializeConsumeRequest(in);
            case Types.TYPE_CREATE_TOPIC: return Serializer.deserializeCreateTopicRequest(in);
            case Types.TYPE_LIST_TOPICS:  return new Types.ListTopicsRequest();
            default: throw new IOException("unknown request type: 0x" + Integer.toHexString(reqType));
        }
    }

    public void writeResponse(Types.Response resp) throws IOException {
        if (resp instanceof Types.ProduceResponse) {
            Serializer.serializeProduceResponse(out, (Types.ProduceResponse) resp);
        } else if (resp instanceof Types.ConsumeResponse) {
            Serializer.serializeConsumeResponse(out, (Types.ConsumeResponse) resp);
        } else if (resp instanceof Types.CreateTopicResponse) {
            Serializer.serializeCreateTopicResponse(out, (Types.CreateTopicResponse) resp);
        } else if (resp instanceof Types.ListTopicsResponse) {
            Serializer.serializeListTopicsResponse(out, (Types.ListTopicsResponse) resp);
        } else if (resp instanceof Types.ErrorResponse) {
            Serializer.serializeString(out, ((Types.ErrorResponse) resp).error);
        } else {
            throw new IOException("unknown response type: " + resp.getClass().getName());
        }
        out.flush();
    }
}
