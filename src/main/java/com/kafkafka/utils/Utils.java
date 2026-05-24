package com.kafkafka.utils;

import java.io.*;
import java.net.InetSocketAddress;
import java.nio.file.*;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

public class Utils {

    public static List<String> getDirectChildrenDirNames(String dirPath) throws IOException {
        List<String> names = new ArrayList<>();
        File dir = new File(dirPath);
        File[] entries = dir.listFiles();
        if (entries == null) return names;
        for (File f : entries) {
            if (f.isDirectory()) names.add(f.getName());
        }
        return names;
    }

    /**
     * FNV-1a 32-bit hash, matching the Go fnv.New32a implementation.
     */
    public static int hash(String payload) {
        if (payload == null || payload.isEmpty()) return 0;
        int hash = 0x811c9dc5; // FNV offset basis
        for (byte b : payload.getBytes()) {
            hash ^= (b & 0xFF);
            hash *= 0x01000193; // FNV prime
        }
        return hash; // signed int32, same as Go int32(h.Sum32())
    }

    public static void validateDirPath(String dirPath) throws IOException {
        File f = new File(dirPath);
        if (!f.exists())     throw new IOException("base path does not exist: " + dirPath);
        if (!f.isDirectory()) throw new IOException("base path is not a directory: " + dirPath);
    }

    public static void validateTCPAddress(String address) throws IOException {
        if (address == null || address.isEmpty())
            throw new IOException("address cannot be empty");
        String[] parts = address.split(":");
        if (parts.length != 2)
            throw new IOException("invalid TCP address: " + address);
        try {
            int port = Integer.parseInt(parts[1]);
            if (port < 0 || port > 65535)
                throw new IOException("invalid port in address: " + address);
        } catch (NumberFormatException e) {
            throw new IOException("invalid port in address: " + address);
        }
    }
}
