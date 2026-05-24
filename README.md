# Kafkafka

A Kafka-inspired distributed message broker built from scratch in Java. No external dependencies — just the JDK and Maven.

Built to understand how event streaming systems work at the internals level: append-only logs, binary protocols, write batching, and offset-based consumption.

---

## Architecture

```
┌─────────────┐        TCP (binary protocol)        ┌──────────────────────────────────────┐
│  Producer   │ ──────────────────────────────────► │              Broker                  │
└─────────────┘                                      │                                      │
                                                     │  Topic: "my-topic"                   │
┌─────────────┐        TCP (binary protocol)         │  ├── Partition 0                     │
│  Consumer   │ ◄────────────────────────────────── │  │     ├── .log   (binary messages)   │
└─────────────┘                                      │  │     └── .index (offset → byte pos) │
                                                     │  ├── Partition 1                     │
                                                     │  └── Partition 2                     │
                                                     └──────────────────────────────────────┘
```

### Write path
```
Producer.produce("hello")
  → serialize to binary
  → send over TCP
  → Broker receives request
  → hash key → pick partition
  → increment offset
  → push to Batcher queue
  → Batcher flushes to .log every 100ms (or when buffer full)
  → update .index file
  → return partition + offset to producer
```

### Read path
```
Consumer.consume(topic, partition, offset=5, max=10)
  → send over TCP
  → Broker looks up offset 5 in .index → byte position 1024
  → seek to byte 1024 in .log
  → read up to 10 messages sequentially
  → return to consumer
```

---

## Key Design Decisions

### 1. Append-only log
Each partition is a single binary file. Messages are never updated or deleted — only appended. This gives sequential write performance and makes the log immutable and crash-safe.

### 2. Separate index file
The `.index` file maps logical offsets to byte positions in the `.log` file, enabling **O(1) random reads** by offset without scanning the entire log.

```
.index contents:
1:0, 2:52, 3:104, 4:156 ...
     ↑
     byte offset in .log where message #2 starts
```

### 3. Write batching
Writes don't go directly to disk. The `Batcher` accumulates messages in memory and flushes in two cases:
- Every **100ms** (time-based flush)
- When the buffer exceeds **32 messages × 16KB** (size-based flush)

This reduces disk I/O significantly on high-throughput workloads.

### 4. FNV-1a partition routing
When a producer sends a message with a key, the broker computes `FNV-1a(key) % numPartitions` to pick the partition. Same key always lands on the same partition — guaranteeing ordering per key.

### 5. Persistent consumer offsets
The consumer saves its current offset to a local file after every message. On restart it reads the saved offset and resumes exactly where it left off — no messages skipped, no duplicates.

---

## Project Structure

```
src/main/java/com/kafkafka/
│
├── BrokerMain.java          ← start the broker
├── ProducerMain.java        ← interactive producer (type messages)
├── ConsumerMain.java        ← live consumer (polls and prints)
│
├── broker/
│   ├── Broker.java          ← TCP server, topic registry, request routing
│   └── BrokerHandler.java   ← per-connection request loop
│
├── log/
│   ├── Log.java             ← core append-only log
│   ├── Message.java         ← message model (offset, timestamp, payload)
│   ├── MessageSerializer.java  ← binary encode/decode
│   └── batcher/
│       ├── Batcher.java         ← batched async writer
│       └── BatcherSideEffects.java
│
├── partition/
│   └── Partition.java       ← wraps a Log, named by partition id
│
├── topic/
│   └── Topic.java           ← manages N partitions, routes by key hash
│
├── producer/
│   └── Producer.java        ← TCP client for producing
│
├── consumer/
│   └── Consumer.java        ← TCP client for consuming
│
├── protocol/
│   └── BinaryProtocol.java  ← reads/writes requests and responses over socket
│
├── serializer/
│   └── Serializer.java      ← wire serialization for all request/response types
│
├── types/
│   └── Types.java           ← all request/response types and constants
│
└── utils/
    └── Utils.java           ← FNV hash, path validation, directory helpers
```

---

## Getting Started

**Prerequisites:** Java 17+, Maven 3.x

### 1. Clone and build
```bash
git clone https://github.com/YOUR_USERNAME/kafkafka-java.git
cd kafkafka-java
mvn compile
```

### 2. Start the broker (Terminal 1)
```bash
mvn exec:java "-Dexec.mainClass=com.kafkafka.BrokerMain"
```
```
==============================
  Broker RUNNING on port 9092
  Topic 'my-topic' (3 partitions) ready
  Press Ctrl+C to stop
==============================
```

### 3. Start the consumer (Terminal 2)
```bash
mvn exec:java "-Dexec.mainClass=com.kafkafka.ConsumerMain"
```
```
Resuming from offset 1 on partition 0
Waiting for messages...
```

### 4. Produce messages (Terminal 3)
```bash
mvn exec:java "-Dexec.mainClass=com.kafkafka.ProducerMain"
```
```
Connected! Type a message and press Enter. Type 'exit' to quit.

> hello
  ✓ sent to partition=0, offset=1
> world
  ✓ sent to partition=0, offset=2
```

Messages appear in the consumer terminal in real time:
```
[offset=1   ] hello
[offset=2   ] world
```

### Reset data
```bash
# Windows
rmdir /s /q kafka-data
rmdir /s /q consumer-offsets

# Mac / Linux
rm -rf kafka-data consumer-offsets
```

---

## Wire Protocol

All communication is over a raw TCP binary protocol (big-endian).

### Produce request
```
[0x01][topic length: 4B][topic][payload length: 4B][payload][key length: 4B][key]
```

### Consume request
```
[0x02][topic length: 4B][topic][partition: 4B][offset: 8B][max: 2B]
```

### Message on disk
```
[total length: 4B][offset: 8B][timestamp: 8B][payload length: 4B][payload]
```

---

## On-disk layout

After running, the `kafka-data/` directory looks like:
```
kafka-data/
└── my-topic/
    ├── p-00001/
    │   ├── .log      ← raw binary messages, append-only
    │   └── .index    ← "1:0,2:52,3:104,..." offset→bytepos
    ├── p-00002/
    │   ├── .log
    │   └── .index
    └── p-00003/
        ├── .log
        └── .index
```

---

## What's intentionally not implemented

This is a learning project — the following are out of scope:

| Feature | Real Kafka |
|---|---|
| Replication (leader/follower) | ✅ |
| Consumer groups + rebalancing | ✅ |
| Log segment rolling + retention | ✅ |
| Log compaction | ✅ |
| Idempotent producer | ✅ |
| Compression (gzip, snappy) | ✅ |
| TLS / SASL authentication | ✅ |
| Multi-broker cluster | ✅ |
| ZooKeeper / KRaft metadata | ✅ |

---

## Original Go implementation

This project is a Java port of [kafkafka](https://github.com/ORIGINAL_REPO_LINK) — a Kafka clone written in Go. The Java version preserves the same binary wire protocol, making the producer/consumer clients wire-compatible with the original Go broker.

---

## License

MIT