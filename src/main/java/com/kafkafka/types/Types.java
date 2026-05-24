package com.kafkafka.types;

import com.kafkafka.log.Message;
import java.util.List;

public class Types {

    // --- Request type codes ---
    public static final int TYPE_PRODUCE      = 0x01;
    public static final int TYPE_CONSUME      = 0x02;
    public static final int TYPE_CREATE_TOPIC = 0x03;
    public static final int TYPE_LIST_TOPICS  = 0x04;

    // --- Marker interfaces ---
    public interface Request {}
    public interface Response {}

    // --- Requests ---
    public static class ProduceRequest implements Request {
        public String topic;
        public String payload;
        public String key;

        public ProduceRequest(String topic, String payload, String key) {
            this.topic   = topic;
            this.payload = payload;
            this.key     = key;
        }
    }

    public static class ConsumeRequest implements Request {
        public String topic;
        public int    partition;
        public long   offset;
        public short  max;

        public ConsumeRequest(String topic, int partition, long offset, short max) {
            this.topic     = topic;
            this.partition = partition;
            this.offset    = offset;
            this.max       = max;
        }
    }

    public static class CreateTopicRequest implements Request {
        public String topic;
        public short  numPartitions;

        public CreateTopicRequest(String topic, short numPartitions) {
            this.topic         = topic;
            this.numPartitions = numPartitions;
        }
    }

    public static class ListTopicsRequest implements Request {}

    // --- Responses ---
    public static class ProduceResponse implements Response {
        public int    partition;
        public long   offset;
        public String error;

        public ProduceResponse(int partition, long offset, String error) {
            this.partition = partition;
            this.offset    = offset;
            this.error     = error;
        }
    }

    public static class ConsumeResponse implements Response {
        public List<Message> messages;
        public String        error;

        public ConsumeResponse(List<Message> messages, String error) {
            this.messages = messages;
            this.error    = error;
        }
    }

    public static class CreateTopicResponse implements Response {
        public String error;
        public CreateTopicResponse(String error) { this.error = error; }
    }

    public static class TopicInfo {
        public String name;
        public short  numPartitions;

        public TopicInfo(String name, short numPartitions) {
            this.name          = name;
            this.numPartitions = numPartitions;
        }
    }

    public static class ListTopicsResponse implements Response {
        public List<TopicInfo> topics;
        public String          error;

        public ListTopicsResponse(List<TopicInfo> topics, String error) {
            this.topics = topics;
            this.error  = error;
        }
    }

    public static class ErrorResponse implements Response {
        public String error;
        public ErrorResponse(String error) { this.error = error; }
    }
}
