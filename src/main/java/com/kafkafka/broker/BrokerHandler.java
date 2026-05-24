package com.kafkafka.broker;

import com.kafkafka.protocol.BinaryProtocol;
import com.kafkafka.types.Types;

import java.io.*;
import java.net.Socket;

/**
 * Handles a single client connection: reads requests in a loop and routes them.
 * Mirrors the Go handleConnection + route logic.
 */
public class BrokerHandler {

    private final BinaryProtocol protocol;
    private final Broker broker;

    public BrokerHandler(Socket conn, Broker broker) throws IOException {
        this.protocol = new BinaryProtocol(conn);
        this.broker   = broker;
    }

    public void handle() throws IOException {
        while (true) {
            Types.Request req;
            try {
                req = protocol.readRequest();
            } catch (EOFException e) {
                return; // client disconnected
            }

            Types.Response resp = route(req);
            protocol.writeResponse(resp);
        }
    }

    private Types.Response route(Types.Request req) {
        if (req instanceof Types.ProduceRequest)      return broker.handleProduce((Types.ProduceRequest) req);
        if (req instanceof Types.ConsumeRequest)      return broker.handleConsume((Types.ConsumeRequest) req);
        if (req instanceof Types.CreateTopicRequest)  return broker.handleCreateTopic((Types.CreateTopicRequest) req);
        if (req instanceof Types.ListTopicsRequest)   return broker.handleListTopics();
        return new Types.ErrorResponse("unknown request type");
    }
}
