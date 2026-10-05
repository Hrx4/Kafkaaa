package com.kafkafka.config;

import java.io.*;
import java.util.Properties;

/**
 * Loaded from a broker.properties file.
 *
 * Example broker.properties:
 *   broker.id=1
 *   broker.address=localhost:9092
 *   data.dir=kafka-data-1
 *   cluster.brokers=localhost:9092,localhost:9093,localhost:9094
 */
public class BrokerConfig {

    public final int    brokerId;
    public final String address;
    public final String dataDir;
    public final String[] clusterBrokers; // all broker addresses in the cluster

    private BrokerConfig(int brokerId, String address, String dataDir, String[] clusterBrokers) {
        this.brokerId       = brokerId;
        this.address        = address;
        this.dataDir        = dataDir;
        this.clusterBrokers = clusterBrokers;
    }

    public static BrokerConfig load(String filePath) throws IOException {
        Properties props = new Properties();
        try (FileInputStream fis = new FileInputStream(filePath)) {
            props.load(fis);
        }

        int    brokerId = Integer.parseInt(props.getProperty("broker.id", "1"));
        String address  = props.getProperty("broker.address", "localhost:9092");
        String dataDir  = props.getProperty("data.dir", "kafka-data-" + brokerId);
        String brokerList = props.getProperty("cluster.brokers", address);
        String[] clusterBrokers = brokerList.split(",");

        return new BrokerConfig(brokerId, address, dataDir, clusterBrokers);
    }

    /** Creates a config programmatically (for tests / BrokerMain). */
    public static BrokerConfig of(int brokerId, String address, String dataDir, String[] clusterBrokers) {
        return new BrokerConfig(brokerId, address, dataDir, clusterBrokers);
    }

    @Override
    public String toString() {
        return String.format("BrokerConfig{id=%d, address=%s, dataDir=%s, cluster=%s}",
                brokerId, address, dataDir, String.join(",", clusterBrokers));
    }
}