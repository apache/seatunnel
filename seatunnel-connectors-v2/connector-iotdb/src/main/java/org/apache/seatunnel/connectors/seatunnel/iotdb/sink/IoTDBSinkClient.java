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

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iotdb.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iotdb.exception.IotdbConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iotdb.exception.IotdbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iotdb.serialize.IoTDBRecord;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

@Slf4j
public class IoTDBSinkClient {

    private final SinkConfig sinkConfig;
    private final List<IoTDBRecord> batchList;

    private Session session;
    private ScheduledExecutorService scheduler;
    private ScheduledFuture<?> scheduledFuture;
    private volatile boolean initialize;
    private volatile Exception flushException;

    public IoTDBSinkClient(SinkConfig sinkConfig) {
        this.sinkConfig = sinkConfig;
        this.batchList = new ArrayList<>();
    }

    private void tryInit() throws IOException {
        if (initialize) {
            return;
        }

        Session.Builder sessionBuilder = new Session.Builder()
            .nodeUrls(sinkConfig.getNodeUrls())
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

        session = sessionBuilder.build();
        try {
            if (sinkConfig.getConnectionTimeoutInMs() != null) {
                session.open(sinkConfig.getEnableRPCCompression(), sinkConfig.getConnectionTimeoutInMs());
            } else if (sinkConfig.getEnableRPCCompression() != null) {
                session.open(sinkConfig.getEnableRPCCompression());
            } else {
                session.open();
            }
        } catch (IoTDBConnectionException e) {
            log.error("Initialize IoTDB client failed.", e);
            throw new IotdbConnectorException(IotdbConnectorErrorCode.INITIALIZE_CLIENT_FAILED,
                "Initialize IoTDB client failed.", e);
        }

        if (sinkConfig.getBatchIntervalMs() != null) {
            scheduler = Executors.newSingleThreadScheduledExecutor(
                new ThreadFactoryBuilder().setNameFormat("IoTDB-sink-output-%s").build());
            scheduledFuture = scheduler.scheduleAtFixedRate(
                () -> {
                    try {
                        flush();
                    } catch (IOException e) {
                        flushException = e;
                    }
                },
                sinkConfig.getBatchIntervalMs(),
                sinkConfig.getBatchIntervalMs(),
                TimeUnit.MILLISECONDS);
        }
        initialize = true;
    }

    public synchronized void write(IoTDBRecord record) throws IOException {
        tryInit();
        checkFlushException();

        batchList.add(record);
        if (sinkConfig.getBatchSize() > 0
            && batchList.size() >= sinkConfig.getBatchSize()) {
            flush();
        }
    }

    public synchronized void close() throws IOException {
        if (scheduledFuture != null) {
            scheduledFuture.cancel(false);
            scheduler.shutdown();
        }

        flush();

        try {
            if (session != null) {
                session.close();
            }
        } catch (IoTDBConnectionException e) {
            log.error("Close IoTDB client failed.", e);
            throw new IotdbConnectorException(IotdbConnectorErrorCode.CLOSE_CLIENT_FAILED,
                "Close IoTDB client failed.", e);
        }
    }

    synchronized void flush() throws IOException {
        checkFlushException();
        if (batchList.isEmpty()) {
            return;
        }

        BatchRecords batchRecords = new BatchRecords(batchList);
        for (int i = 0; i <= sinkConfig.getMaxRetries(); i++) {
            try {
                if (batchRecords.getTypesList().isEmpty()) {
                    session.insertRecords(batchRecords.getDeviceIds(),
                        batchRecords.getTimestamps(),
                        batchRecords.getMeasurementsList(),
                        batchRecords.getStringValuesList());
                } else {
                    session.insertRecords(batchRecords.getDeviceIds(),
                        batchRecords.getTimestamps(),
                        batchRecords.getMeasurementsList(),
                        batchRecords.getTypesList(),
                        batchRecords.getValuesList());
                }
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                log.error("Writing records to IoTDB failed, retry times = {}", i, e);
                if (i >= sinkConfig.getMaxRetries()) {
                    throw new IotdbConnectorException(CommonErrorCode.FLUSH_DATA_FAILED,
                        "Writing records to IoTDB failed.", e);
                }

                try {
                    long backoff = Math.min(sinkConfig.getRetryBackoffMultiplierMs() * i,
                        sinkConfig.getMaxRetryBackoffMs());
                    Thread.sleep(backoff);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new IotdbConnectorException(CommonErrorCode.FLUSH_DATA_FAILED,
                        "Unable to flush; interrupted while doing another attempt.", e);
                }
            }
        }

        batchList.clear();
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new IotdbConnectorException(CommonErrorCode.FLUSH_DATA_FAILED,
                "Writing records to IoTDB failed.", flushException);
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
