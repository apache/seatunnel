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

package org.apache.seatunnel.connectors.seatunnel.iotdb.sink;

import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iotdb.exception.IotdbConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iotdb.exception.IotdbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iotdb.serialize.IoTDBRecord;

import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class IoTDBSinkClient {

    /** TSStatusCode.REDIRECTION_RECOMMEND: the node is not the leader of the target region. */
    private static final int REDIRECTION_RECOMMEND = 400;

    private static final int DEFAULT_IOTDB_PORT = 6667;

    private final SinkConfig sinkConfig;
    private final List<IoTDBRecord> batchList;

    /** One lazily-created session per configured node url. */
    private final Session[] sessions;

    /**
     * Device -> index in nodeUrls of the node that last accepted its writes (the region leader).
     * Writes are grouped by this so each group goes straight to the right leader.
     */
    private final Map<String, Integer> deviceLeaderIndex = new HashMap<>();

    /**
     * Devices whose leader is not known yet (or just changed). They are written one device per
     * request until a node accepts them, because a batch mixing devices of different regions is
     * rejected as a whole. Once accepted, the device goes back to normal batched routing.
     */
    private final Set<String> probingDevices = new HashSet<>();

    private volatile boolean initialize;
    private volatile Exception flushException;

    public IoTDBSinkClient(SinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        this.batchList = new ArrayList<>();
        this.sessions = new Session[sinkConfig.getNodeUrls().size()];
    }

    private void tryInit() throws IOException {
        if (initialize) {
            return;
        }
        try {
            sessionFor(0);
        } catch (IoTDBConnectionException e) {
            log.error("Initialize IoTDB client failed.", e);
            throw new IotdbConnectorException(
                    IotdbConnectorErrorCode.INITIALIZE_CLIENT_FAILED,
                    "Initialize IoTDB client failed.",
                    e);
        }
        initialize = true;
    }

    private Session sessionFor(int index) throws IoTDBConnectionException {
        if (sessions[index] != null) {
            return sessions[index];
        }
        String nodeUrl = sinkConfig.getNodeUrls().get(index);
        String host = nodeUrl;
        int port = DEFAULT_IOTDB_PORT;
        int colonIndex = nodeUrl.lastIndexOf(':');
        if (colonIndex > 0) {
            host = nodeUrl.substring(0, colonIndex);
            port = Integer.parseInt(nodeUrl.substring(colonIndex + 1));
        }
        Session session = buildSession(host, port);
        sessions[index] = session;
        return session;
    }

    /** Builds and opens the session to one node; overridable for tests. */
    protected Session buildSession(String host, int port) throws IoTDBConnectionException {
        Session.Builder sessionBuilder =
                new Session.Builder()
                        .host(host)
                        .port(port)
                        .username(sinkConfig.getUsername())
                        .password(sinkConfig.getPassword());
        if (sinkConfig.getThriftDefaultBufferSize() != null) {
            sessionBuilder.thriftDefaultBufferSize(sinkConfig.getThriftDefaultBufferSize());
        }
        if (sinkConfig.getThriftMaxFrameSize() != null) {
            sessionBuilder.thriftMaxFrameSize(sinkConfig.getThriftMaxFrameSize());
        }
        if (sinkConfig.getZoneId() != null) {
            sessionBuilder.zoneId(sinkConfig.getZoneId());
        }

        Session session = sessionBuilder.build();
        if (sinkConfig.getConnectionTimeoutInMs() != null) {
            session.open(
                    sinkConfig.getEnableRPCCompression(), sinkConfig.getConnectionTimeoutInMs());
        } else if (sinkConfig.getEnableRPCCompression() != null) {
            session.open(sinkConfig.getEnableRPCCompression());
        } else {
            session.open();
        }
        return session;
    }

    private void closeSessionQuietly(int index) {
        Session stale = sessions[index];
        sessions[index] = null;
        if (stale != null) {
            try {
                stale.close();
            } catch (Exception e) {
                log.warn(
                        "Close stale IoTDB session for {} failed.",
                        sinkConfig.getNodeUrls().get(index),
                        e);
            }
        }
    }

    public synchronized void write(IoTDBRecord record) throws IOException {
        tryInit();
        checkFlushException();

        batchList.add(record);
        if (sinkConfig.getBatchSize() > 0 && batchList.size() >= sinkConfig.getBatchSize()) {
            flush();
        }
    }

    public synchronized void close() throws IOException {
        flush();

        for (int i = 0; i < sessions.length; i++) {
            if (sessions[i] != null) {
                try {
                    sessions[i].close();
                } catch (IoTDBConnectionException e) {
                    log.error("Close IoTDB client failed.", e);
                    throw new IotdbConnectorException(
                            IotdbConnectorErrorCode.CLOSE_CLIENT_FAILED,
                            "Close IoTDB client failed.",
                            e);
                }
            }
        }
    }

    /**
     * Writes are routed per device to the node that last accepted them (the region leader). A
     * REDIRECTION_RECOMMEND reply means the current node is no longer the leader for those devices:
     * the affected devices are re-routed to another node and retried immediately, without waiting.
     * Only network-level failures go through backoff waiting.
     */
    synchronized void flush() throws IOException {
        checkFlushException();
        if (batchList.isEmpty()) {
            return;
        }

        List<IoTDBRecord> remaining = new ArrayList<>(batchList);
        Exception[] lastError = new Exception[1];
        int nodeCount = sessions.length;
        // Redirects rotate endpoints without consuming the retry budget; the round bound only
        // exists so a leaderless or broken cluster can never spin this loop forever.
        int maxRounds = nodeCount + retryBudget() + 1;

        for (int round = 0; !remaining.isEmpty(); round++) {
            if (round >= maxRounds) {
                throw new IotdbConnectorException(
                        CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                        buildFailureMessage(remaining, lastError[0]),
                        lastError[0]);
            }

            // Batched groups by cached leader, plus one single-device group per probing device.
            Map<Integer, List<IoTDBRecord>> endpointGroups = new LinkedHashMap<>();
            Map<String, List<IoTDBRecord>> probeGroups = new LinkedHashMap<>();
            for (IoTDBRecord record : remaining) {
                String device = record.getDevice();
                if (probingDevices.contains(device)) {
                    probeGroups.computeIfAbsent(device, k -> new ArrayList<>()).add(record);
                } else {
                    endpointGroups
                            .computeIfAbsent(leaderIndexOf(device), k -> new ArrayList<>())
                            .add(record);
                }
            }

            List<IoTDBRecord> nextRound = new ArrayList<>();
            boolean networkFailure = false;
            for (Map.Entry<Integer, List<IoTDBRecord>> group : endpointGroups.entrySet()) {
                networkFailure |=
                        writeGroup(group.getKey(), group.getValue(), nextRound, round, lastError);
            }
            for (Map.Entry<String, List<IoTDBRecord>> group : probeGroups.entrySet()) {
                networkFailure |=
                        writeGroup(
                                leaderIndexOf(group.getKey()),
                                group.getValue(),
                                nextRound,
                                round,
                                lastError);
            }

            if (nextRound.isEmpty()) {
                break;
            }
            remaining = nextRound;
            if (networkFailure) {
                sleepBackoff(round);
            }
        }

        batchList.clear();
    }

    /**
     * Writes one group to the given node. Returns true when the failure was network-level (the
     * caller then backs off before the next round). Records that must be retried are appended to
     * nextRound.
     */
    private boolean writeGroup(
            int index,
            List<IoTDBRecord> records,
            List<IoTDBRecord> nextRound,
            int round,
            Exception[] lastError)
            throws IOException {
        try {
            insertRecords(sessionFor(index), records);
            for (IoTDBRecord record : records) {
                probingDevices.remove(record.getDevice());
            }
            return false;
        } catch (StatementExecutionException e) {
            if (isRedirectionRecommendation(e)) {
                Set<String> devices = new HashSet<>();
                for (IoTDBRecord record : records) {
                    devices.add(record.getDevice());
                }
                if (devices.size() > 1) {
                    // The batch mixes devices of different regions, so the node rejected the
                    // whole request; each device must find its own leader first.
                    log.info(
                            "IoTDB node {} rejected a batch of {} records mixing {} devices;"
                                    + " switching them to per-device routing.",
                            sinkConfig.getNodeUrls().get(index),
                            records.size(),
                            devices.size());
                    probingDevices.addAll(devices);
                } else {
                    String device = devices.iterator().next();
                    log.info(
                            "IoTDB node {} is not the leader for device {}; re-routing to the"
                                    + " next configured node.",
                            sinkConfig.getNodeUrls().get(index),
                            device);
                    deviceLeaderIndex.put(device, (index + 1) % sessions.length);
                    probingDevices.add(device);
                }
                nextRound.addAll(records);
                lastError[0] = e;
                return false;
            }
            // The server rejected the data itself; neither waiting nor re-routing to another
            // node can fix that.
            throw new IotdbConnectorException(
                    CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                    buildFailureMessage(records, e),
                    e);
        } catch (IoTDBConnectionException e) {
            log.warn(
                    "Writing {} records to IoTDB node {} failed (attempt {});"
                            + " rebuilding session and retrying.",
                    records.size(),
                    sinkConfig.getNodeUrls().get(index),
                    round + 1,
                    e);
            closeSessionQuietly(index);
            nextRound.addAll(records);
            lastError[0] = e;
            return true;
        }
    }

    private void insertRecords(Session session, List<IoTDBRecord> records)
            throws IoTDBConnectionException, StatementExecutionException {
        BatchRecords batchRecords = new BatchRecords(records);
        if (batchRecords.getTypesList().isEmpty()) {
            session.insertRecords(
                    batchRecords.getDeviceIds(),
                    batchRecords.getTimestamps(),
                    batchRecords.getMeasurementsList(),
                    batchRecords.getStringValuesList());
        } else {
            session.insertRecords(
                    batchRecords.getDeviceIds(),
                    batchRecords.getTimestamps(),
                    batchRecords.getMeasurementsList(),
                    batchRecords.getTypesList(),
                    batchRecords.getValuesList());
        }
    }

    private int leaderIndexOf(String device) {
        Integer index = deviceLeaderIndex.get(device);
        return index == null ? 0 : index;
    }

    private static boolean isRedirectionRecommendation(StatementExecutionException e) {
        String message = e.getMessage();
        if (message == null || message.isEmpty()) {
            return false;
        }
        int colonIndex = message.indexOf(':');
        String code = colonIndex > 0 ? message.substring(0, colonIndex) : message;
        try {
            return Integer.parseInt(code.trim()) == REDIRECTION_RECOMMEND;
        } catch (NumberFormatException ex) {
            return false;
        }
    }

    private int retryBudget() {
        return Math.max(0, sinkConfig.getMaxRetries());
    }

    private void sleepBackoff(int round) throws IOException {
        long multiplier = sinkConfig.getRetryBackoffMultiplierMs();
        if (multiplier <= 0) {
            return;
        }
        long backoff = Math.min(multiplier * (round + 1L), sinkConfig.getMaxRetryBackoffMs());
        try {
            Thread.sleep(backoff);
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IotdbConnectorException(
                    CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                    "Unable to flush; interrupted while waiting to retry IoTDB write.",
                    ex);
        }
    }

    private static String buildFailureMessage(List<IoTDBRecord> records, Exception cause) {
        IoTDBRecord first = records.get(0);
        long firstTimestamp = first.getTimestamp() == null ? -1L : first.getTimestamp();
        return String.format(
                "Writing %d records to IoTDB failed; first device: %s; first timestamp: %d;"
                        + " cause: %s",
                records.size(),
                first.getDevice(),
                firstTimestamp,
                cause == null ? "unknown" : cause.getMessage());
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new IotdbConnectorException(
                    CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                    "Writing records to IoTDB failed.",
                    flushException);
        }
    }

    @Getter
    private static class BatchRecords {
        private final List<String> deviceIds;
        private final List<Long> timestamps;
        private final List<List<String>> measurementsList;
        private final List<List<TSDataType>> typesList;
        private final List<List<Object>> valuesList;

        public BatchRecords(List<IoTDBRecord> batchList) {
            int batchSize = batchList.size();
            this.deviceIds = new ArrayList<>(batchSize);
            this.timestamps = new ArrayList<>(batchSize);
            this.measurementsList = new ArrayList<>(batchSize);
            this.typesList = new ArrayList<>(batchSize);
            this.valuesList = new ArrayList<>(batchSize);

            for (IoTDBRecord record : batchList) {
                deviceIds.add(record.getDevice());
                timestamps.add(record.getTimestamp());
                measurementsList.add(record.getMeasurements());
                if (record.getTypes() != null && !record.getTypes().isEmpty()) {
                    typesList.add(record.getTypes());
                }
                valuesList.add(record.getValues());
            }
        }

        private List<List<String>> getStringValuesList() {
            List<?> tmp = valuesList;
            return (List<List<String>>) tmp;
        }
    }
}
