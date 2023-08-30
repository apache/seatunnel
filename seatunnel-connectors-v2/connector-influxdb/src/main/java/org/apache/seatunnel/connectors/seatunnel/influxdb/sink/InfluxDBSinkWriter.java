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

package org.apache.seatunnel.connectors.seatunnel.influxdb.sink;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.influxdb.client.InfluxDBClient;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.influxdb.exception.InfluxdbConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.influxdb.exception.InfluxdbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.influxdb.serialize.DefaultSerializer;
import org.apache.seatunnel.connectors.seatunnel.influxdb.serialize.Serializer;

import org.influxdb.InfluxDB;
import org.influxdb.dto.BatchPoints;
import org.influxdb.dto.Point;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
public class InfluxDBSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final Serializer serializer;
    private InfluxDB influxdb;
    private final SinkConfig sinkConfig;
    private final List<Point> batchList;
    private volatile Exception flushException;

    public InfluxDBSinkWriter(Config pluginConfig, SeaTunnelRowType seaTunnelRowType)
            throws ConnectException {
        this.sinkConfig = SinkConfig.loadConfig(pluginConfig);
        this.serializer =
                new DefaultSerializer(
                        seaTunnelRowType,
                        sinkConfig.getPrecision().getTimeUnit(),
                        sinkConfig.getKeyTags(),
                        sinkConfig.getKeyTime(),
                        sinkConfig.getMeasurement());
        this.batchList = new ArrayList<>();

        connect();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        Point record = serializer.serialize(element);
        write(record);
    }

    @SneakyThrows
    @Override
    public Optional<Void> prepareCommit() {
        // Flush to storage before snapshot state is performed
        flush();
        return super.prepareCommit();
    }

    @Override
    public void close() throws IOException {
        flush();

        if (influxdb != null) {
            influxdb.close();
            influxdb = null;
        }
    }

    public void write(Point record) throws IOException {
        checkFlushException();

        batchList.add(record);
        if (sinkConfig.getBatchSize() > 0 && batchList.size() >= sinkConfig.getBatchSize()) {
            flush();
        }
    }

    public void flush() throws IOException {
        checkFlushException();
        if (batchList.isEmpty()) {
            return;
        }
        BatchPoints.Builder batchPoints = BatchPoints.database(sinkConfig.getDatabase());
        for (int i = 0; i <= sinkConfig.getMaxRetries(); i++) {
            try {
                batchPoints.points(batchList);
                influxdb.write(batchPoints.build());
            } catch (Exception e) {
                log.error("Writing records to influxdb failed, retry times = {}", i, e);
                if (i >= sinkConfig.getMaxRetries()) {
                    throw new InfluxdbConnectorException(
                            CommonErrorCode.FLUSH_DATA_FAILED,
                            "Writing records to InfluxDB failed.",
                            e);
                }

                try {
                    long backoff =
                            Math.min(
                                    sinkConfig.getRetryBackoffMultiplierMs() * i,
                                    sinkConfig.getMaxRetryBackoffMs());
                    Thread.sleep(backoff);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                    throw new InfluxdbConnectorException(
                            CommonErrorCode.FLUSH_DATA_FAILED,
                            "Unable to flush; interrupted while doing another attempt.",
                            e);
                }
            }
        }

        batchList.clear();
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new InfluxdbConnectorException(
                    CommonErrorCode.FLUSH_DATA_FAILED,
                    "Writing records to InfluxDB failed.",
                    flushException);
        }
    }

    public void connect() throws ConnectException {
        if (influxdb == null) {
            influxdb = InfluxDBClient.getWriteClient(sinkConfig);
            String version = influxdb.version();
            if (!influxdb.ping().isGood()) {
                throw new InfluxdbConnectorException(
                        InfluxdbConnectorErrorCode.CONNECT_FAILED,
                        String.format(
                                "connect influxdb failed, due to influxdb version info is unknown, the url is: {%s}",
                                sinkConfig.getUrl()));
            }
            log.info("connect influxdb successful. sever version :{}.", version);
        }
    }
}
