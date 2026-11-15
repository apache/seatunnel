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

package org.apache.seatunnel.connectors.seatunnel.influxdb.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.influxdb.client.InfluxDBClient;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBConfig;
import org.apache.seatunnel.connectors.seatunnel.influxdb.converter.InfluxDBRowConverter;
import org.apache.seatunnel.connectors.seatunnel.influxdb.exception.InfluxdbConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.influxdb.exception.InfluxdbConnectorException;

import org.apache.commons.collections4.CollectionUtils;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;

import lombok.extern.slf4j.Slf4j;

import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

@Slf4j
public class InfluxdbSourceReader implements SourceReader<SeaTunnelRow, InfluxDBSourceSplit> {
    private InfluxDB influxdb;
    InfluxDBConfig config;

    private final SourceReader.Context context;

    private final SeaTunnelRowType seaTunnelRowType;

    List<Integer> columnsIndexList;
    private final Map<String, InfluxDBSourceTable> tables;
    private final Queue<InfluxDBSourceSplit> pendingSplits;

    private volatile boolean noMoreSplitsAssignment;

    InfluxdbSourceReader(
            InfluxDBConfig config,
            Context readerContext,
            SeaTunnelRowType seaTunnelRowType,
            List<Integer> columnsIndexList) {
        this.config = config;
        this.pendingSplits = new LinkedList<>();
        this.context = readerContext;
        this.seaTunnelRowType = seaTunnelRowType;
        this.columnsIndexList = columnsIndexList;
        this.tables = Collections.emptyMap();
    }

    InfluxdbSourceReader(
            InfluxDBConfig config, Context readerContext, List<InfluxDBSourceTable> tables) {
        this.config = config;
        this.context = readerContext;
        this.pendingSplits = new LinkedList<>();
        this.seaTunnelRowType = null;
        this.tables = new LinkedHashMap<>();
        for (InfluxDBSourceTable table : tables) {
            this.tables.put(table.getTableId(), table);
        }
    }

    public void connect() throws ConnectException {
        if (influxdb == null) {
            influxdb = InfluxDBClient.getInfluxDB(config);
            String version = influxdb.version();
            if (!influxdb.ping().isGood()) {
                throw new InfluxdbConnectorException(
                        InfluxdbConnectorErrorCode.CONNECT_FAILED,
                        String.format(
                                "connect influxdb failed, due to influxdb version info is unknown, the url is: {%s}",
                                config.getUrl()));
            }
            log.info("connect influxdb successful. sever version :{}.", version);
        }
    }

    @Override
    public void open() throws Exception {
        connect();
    }

    @Override
    public void close() {
        if (influxdb != null) {
            influxdb.close();
            influxdb = null;
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) {
        while (!pendingSplits.isEmpty()) {
            synchronized (output.getCheckpointLock()) {
                InfluxDBSourceSplit split = pendingSplits.poll();
                read(split, output);
            }
        }

        if (Boundedness.BOUNDED.equals(context.getBoundedness())
                && noMoreSplitsAssignment
                && pendingSplits.isEmpty()) {
            // signal to the source that we have reached the end of the data.
            log.info("Closed the bounded influxDB source");
            context.signalNoMoreElement();
        }
    }

    @Override
    public List<InfluxDBSourceSplit> snapshotState(long checkpointId) {
        return new ArrayList<>(pendingSplits);
    }

    @Override
    public void addSplits(List<InfluxDBSourceSplit> splits) {
        pendingSplits.addAll(splits);
    }

    @Override
    public void handleNoMoreSplits() {
        log.info("Reader received NoMoreSplits event.");
        noMoreSplitsAssignment = true;
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    private void read(InfluxDBSourceSplit split, Collector<SeaTunnelRow> output) {
        InfluxDBSourceTable table = null;
        if (!tables.isEmpty()) {
            table = tables.get(split.getTableId());
            if (table == null) {
                throw new InfluxdbConnectorException(
                        CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                        "Unknown table identity in InfluxDB split: " + split.getTableId());
            }
        } else if (split.getTableId() != null) {
            throw new InfluxdbConnectorException(
                    CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                    "Cannot restore a multi-table split with a single-table configuration");
        }
        SeaTunnelRowType rowType =
                table == null ? seaTunnelRowType : table.getCatalogTable().getSeaTunnelRowType();
        String database =
                table == null ? config.getDatabase() : table.getSourceConfig().getDatabase();
        QueryResult queryResult = influxdb.query(new Query(split.getQuery(), database));
        if (table != null && queryResult.hasError()) {
            throw new InfluxdbConnectorException(
                    CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                    "InfluxDB query failed for table "
                            + table.getTableId()
                            + ": "
                            + queryResult.getError());
        }
        List<QueryResult.Result> results = queryResult.getResults();
        if (CollectionUtils.isEmpty(results)) {
            log.debug("split[{}] reader influxDB query result is empty.", split.splitId());
            return;
        }
        for (QueryResult.Result result : results) {
            if (table != null && result.hasError()) {
                throw new InfluxdbConnectorException(
                        CommonErrorCodeDeprecated.SQL_OPERATION_FAILED,
                        "InfluxDB query failed for table "
                                + table.getTableId()
                                + ": "
                                + result.getError());
            }
            List<QueryResult.Series> serieList = result.getSeries();
            if (CollectionUtils.isNotEmpty(serieList)) {
                for (QueryResult.Series series : serieList) {
                    List<Integer> indexes = columnsIndexList;
                    if (table != null) {
                        indexes = new ArrayList<>(rowType.getTotalFields());
                        for (String field : rowType.getFieldNames()) {
                            int index = series.getColumns().indexOf(field);
                            if (index < 0) {
                                throw new InfluxdbConnectorException(
                                        InfluxdbConnectorErrorCode.GET_COLUMN_INDEX_FAILED,
                                        "Missing query column '"
                                                + field
                                                + "' for table "
                                                + table.getTableId());
                            }
                            indexes.add(index);
                        }
                    }
                    for (List<Object> values : series.getValues()) {
                        SeaTunnelRow row = InfluxDBRowConverter.convert(values, rowType, indexes);
                        if (table != null) {
                            row.setTableId(split.getTableId());
                        }
                        output.collect(row);
                    }
                }
            } else {
                log.debug("split[{}] reader influxDB series is empty.", split.splitId());
            }
        }
    }
}
