/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.mysql;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.StringRangeSplitDecision;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.security.MessageDigest;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

@Slf4j
public class MysqlDialectTest {

    @Test
    public void testValidateStringRangeSplitAcceptsPrintableAsciiPunctuation() throws Exception {
        MysqlDialect dialect = new MysqlDialect();
        JdbcSourceTable table =
                JdbcSourceTable.builder().tablePath(TablePath.of("test_db", "test_table")).build();

        StringRangeSplitDecision decision =
                dialect.validateStringRangeSplit(
                        mockConnection("utf8mb4_bin", Arrays.asList("AA/01", "AB.CD", "ID~99")),
                        table,
                        "id",
                        3);

        Assertions.assertTrue(decision.isSafe());
    }

    @Test
    public void testHashDistributionMD5vsCRC32WithSnowflakeIds() {
        int totalRecords = 1_100_000;
        int partitions = 10;
        List<String> snowflakeIds = generateSnowflakeIds(totalRecords);

        Map<Integer, Integer> md5Distribution = new HashMap<>();
        for (int i = 0; i < partitions; i++) {
            md5Distribution.put(i, 0);
        }

        for (String id : snowflakeIds) {
            int partition = calculateMD5Partition(id, partitions);
            md5Distribution.put(partition, md5Distribution.get(partition) + 1);
        }

        Map<Integer, Integer> crc32Distribution = new HashMap<>();
        for (int i = 0; i < partitions; i++) {
            crc32Distribution.put(i, 0);
        }

        for (String id : snowflakeIds) {
            int partition = calculateCRC32Partition(id, partitions);
            crc32Distribution.put(partition, crc32Distribution.get(partition) + 1);
        }

        log.info("MD5 Distribution (OLD - Has Issue):");
        for (int i = 0; i < partitions; i++) {
            int count = md5Distribution.get(i);
            double percentage = (count * 100.0) / totalRecords;
            log.info(
                    String.format(
                            "  Partition %d: %,7d records (%.2f%%)%s",
                            i, count, percentage, (percentage > 20 ? " SKEWED!" : "")));
        }

        log.info("CRC32 Distribution (NEW - Fixed):");
        for (int i = 0; i < partitions; i++) {
            int count = crc32Distribution.get(i);
            double percentage = (count * 100.0) / totalRecords;
            log.info(
                    String.format(
                            "  Partition %d: %,7d records (%.2f%%)%s",
                            i, count, percentage, (percentage > 20 ? " SKEWED!" : "")));
        }

        // Verify that MD5 is severely skewed
        double md5Partition0Percentage = (md5Distribution.get(0) * 100.0) / totalRecords;
        Assertions.assertTrue(md5Partition0Percentage > 30);

        // Verify that CRC32 is evenly distributed
        for (int i = 0; i < partitions; i++) {
            double crc32Percentage = (crc32Distribution.get(i) * 100.0) / totalRecords;
            Assertions.assertTrue(crc32Percentage >= 7 && crc32Percentage <= 13);
        }

        double md5StdDev = calculateStandardDeviation(md5Distribution, totalRecords, partitions);
        double crc32StdDev =
                calculateStandardDeviation(crc32Distribution, totalRecords, partitions);

        // The standard deviation of CRC32 should be much smaller than MD5
        Assertions.assertTrue(crc32StdDev < md5StdDev / 2);
    }

    /** Generate Snowflake Algorithm ID */
    private List<String> generateSnowflakeIds(int count) {
        List<String> ids = new ArrayList<>(count);
        long baseTimestamp = 1704067200000L;
        long timestampBits = baseTimestamp << 22;

        for (int i = 0; i < count; i++) {
            long timeIncrement = (i / 4096) << 22;
            long machineId = (i % 1024) << 12;
            long sequence = i % 4096;

            long snowflakeId = timestampBits + timeIncrement + machineId + sequence;
            ids.add(String.valueOf(snowflakeId));
        }

        return ids;
    }

    /** Simulate the MD5 behavior of MySQL */
    private int calculateMD5Partition(String id, int mod) {
        try {
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(id.getBytes());

            StringBuilder hexString = new StringBuilder();
            for (byte b : digest) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) {
                    hexString.append('0');
                }
                hexString.append(hex);
            }

            String hexResult = hexString.toString();
            long numericValue = convertHexStringToNumberMySQLWay(hexResult);

            return (int) Math.abs(numericValue % mod);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Simulate MySQL string to number conversion: Read from left to right and stop when the first
     * non numeric character is encountered.
     */
    private long convertHexStringToNumberMySQLWay(String hexString) {
        if (hexString == null || hexString.isEmpty()) {
            return 0;
        }

        StringBuilder numericPart = new StringBuilder();
        for (char c : hexString.toCharArray()) {
            if (c >= '0' && c <= '9') {
                numericPart.append(c);
            } else {
                break;
            }
        }

        if (numericPart.length() == 0) {
            return 0;
        }

        try {
            return Long.parseLong(numericPart.toString());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /** Simulate CRC32 behavior */
    private int calculateCRC32Partition(String id, int mod) {
        CRC32 crc32 = new CRC32();
        crc32.update(id.getBytes());
        long crcValue = crc32.getValue();

        return (int) Math.abs(crcValue % mod);
    }

    private double calculateStandardDeviation(
            Map<Integer, Integer> distribution, int totalRecords, int partitions) {
        double mean = totalRecords / (double) partitions;
        double sumSquaredDiff = 0;

        for (int i = 0; i < partitions; i++) {
            double diff = distribution.get(i) - mean;
            sumSquaredDiff += diff * diff;
        }

        return Math.sqrt(sumSquaredDiff / partitions);
    }

    private Connection mockConnection(String collation, List<String> samples) {
        return (Connection)
                java.lang.reflect.Proxy.newProxyInstance(
                        Connection.class.getClassLoader(),
                        new Class<?>[] {Connection.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "prepareStatement":
                                    return mockPreparedStatement(collation);
                                case "createStatement":
                                    return mockStatement(samples);
                                default:
                                    return defaultValue(method.getReturnType());
                            }
                        });
    }

    private PreparedStatement mockPreparedStatement(String collation) {
        return (PreparedStatement)
                java.lang.reflect.Proxy.newProxyInstance(
                        PreparedStatement.class.getClassLoader(),
                        new Class<?>[] {PreparedStatement.class},
                        (proxy, method, args) -> {
                            if ("executeQuery".equals(method.getName())) {
                                return mockResultSet(Arrays.asList(collation));
                            }
                            return defaultValue(method.getReturnType());
                        });
    }

    private Statement mockStatement(List<String> samples) {
        return (Statement)
                java.lang.reflect.Proxy.newProxyInstance(
                        Statement.class.getClassLoader(),
                        new Class<?>[] {Statement.class},
                        (proxy, method, args) -> {
                            if ("executeQuery".equals(method.getName())) {
                                return mockResultSet(samples);
                            }
                            return defaultValue(method.getReturnType());
                        });
    }

    private ResultSet mockResultSet(List<String> values) {
        Iterator<String> iterator = values.iterator();
        String[] current = new String[1];
        return (ResultSet)
                java.lang.reflect.Proxy.newProxyInstance(
                        ResultSet.class.getClassLoader(),
                        new Class<?>[] {ResultSet.class},
                        (proxy, method, args) -> {
                            switch (method.getName()) {
                                case "next":
                                    if (iterator.hasNext()) {
                                        current[0] = iterator.next();
                                        return true;
                                    }
                                    return false;
                                case "getString":
                                    return current[0];
                                default:
                                    return defaultValue(method.getReturnType());
                            }
                        });
    }

    private Object defaultValue(Class<?> returnType) {
        if (!returnType.isPrimitive()) {
            return null;
        }
        if (boolean.class.equals(returnType)) {
            return false;
        }
        if (void.class.equals(returnType)) {
            return null;
        }
        return 0;
    }
}
