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

package org.apache.seatunnel.connectors.seatunnel.iotdbv2.sink.relational;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.config.SinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.exception.IotdbConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.exception.IotdbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.serialize.relational.IoTDBv2RelationalRecord;

import lombok.extern.slf4j.Slf4j;
import shaded.org.apache.iotdb.isession.ITableSession;
import shaded.org.apache.iotdb.rpc.IoTDBConnectionException;
import shaded.org.apache.iotdb.rpc.StatementExecutionException;
import shaded.org.apache.iotdb.session.TableSessionBuilder;
import shaded.org.apache.tsfile.enums.ColumnCategory;
import shaded.org.apache.tsfile.enums.TSDataType;
import shaded.org.apache.tsfile.write.record.Tablet;

import java.io.IOException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class IoTDBv2RelationalSinkClient {

    private final SinkConfig sinkConfig;
    private final List<Tablet> batchList;

    private ITableSession tableSession;

    private volatile boolean initialize;
    private volatile Exception flushException;
    private volatile int curBatchSize;

    private final List<String> tableNameList;
    private final List<String> columnNames;
    private final List<ColumnCategory> columnCategories;
    private final List<TSDataType> columnDataTypes;

    public IoTDBv2RelationalSinkClient(
            SinkConfig sinkConfig,
            List<String> tagKeys,
            List<String> attributeKeys,
            List<String> fieldNames,
            List<TSDataType> fieldTypes) {
        this.sinkConfig = sinkConfig;
        this.batchList = new ArrayList<>();

        int tagSize = tagKeys.size();
        int attributeSize = attributeKeys.size();
        int fieldSize = fieldNames.size();
        this.columnNames = combineColumnNames(tagKeys, attributeKeys, fieldNames);
        this.columnCategories = generateColumnCategories(tagSize, attributeSize, fieldSize);
        this.columnDataTypes = generateColumnTypes(tagSize, attributeSize, fieldTypes);
        this.tableNameList = new ArrayList<>();
    }

    private void tryInit() throws IOException {
        if (initialize) {
            return;
        }

        String database = sinkConfig.getStorageGroup();
        TableSessionBuilder sessionBuilder =
                new TableSessionBuilder()
                        .nodeUrls(sinkConfig.getNodeUrls())
                        .username(sinkConfig.getUsername())
                        .password(sinkConfig.getPassword())
                        .database(database)
                        .enableCompression(false);
        if (sinkConfig.getThriftDefaultBufferSize() != null) {
            sessionBuilder.thriftDefaultBufferSize(sinkConfig.getThriftDefaultBufferSize());
        }
        if (sinkConfig.getThriftMaxFrameSize() != null) {
            sessionBuilder.thriftMaxFrameSize(sinkConfig.getThriftMaxFrameSize());
        }
        if (sinkConfig.getZoneId() != null) {
            sessionBuilder.zoneId(sinkConfig.getZoneId());
        }
        if (sinkConfig.getConnectionTimeoutInMs() != null) {
            sessionBuilder.connectionTimeoutInMs(sinkConfig.getConnectionTimeoutInMs());
        }

        try {
            tableSession = sessionBuilder.build();
        } catch (IoTDBConnectionException e) {
            log.error("Initialize IoTDB client failed.", e);
            throw new IotdbConnectorException(
                    IotdbConnectorErrorCode.INITIALIZE_CLIENT_FAILED,
                    "Initialize IoTDB client failed.",
                    e);
        }

        try {
            tableSession.executeNonQueryStatement("create database if not exists " + database);
        } catch (IoTDBConnectionException | StatementExecutionException e) {
            log.error("Create database failed.", e);
            throw new IotdbConnectorException(
                    IotdbConnectorErrorCode.INITIALIZE_CLIENT_FAILED,
                    "Initialize IoTDB client failed.",
                    e);
        }

        initialize = true;
        curBatchSize = 0;
    }

    public synchronized void write(IoTDBv2RelationalRecord record) throws IOException {
        tryInit();
        checkFlushException();

        String tableName = record.getTableName();
        Tablet curTablet;
        int tabletIndex = tableNameList.indexOf(tableName);
        if (tabletIndex == -1) {
            tableNameList.add(tableName);
            curTablet = new Tablet(tableName, columnNames, columnDataTypes, columnCategories);
            addValuesToTablet(record, curTablet, 0);
            batchList.add(curTablet);
        } else {
            curTablet = batchList.get(tabletIndex);
            addValuesToTablet(record, curTablet, curTablet.getRowSize());
        }
        curBatchSize += 1;

        int batchSize = sinkConfig.getBatchSize();
        if (batchSize > 0 && curBatchSize >= batchSize) {
            flush();
        }
    }

    public void addValuesToTablet(IoTDBv2RelationalRecord record, Tablet tablet, int rowIndex) {
        tablet.addTimestamp(rowIndex, record.getTimestamp());
        int columnIndex = 0;
        for (String tag : record.getTags()) {
            tablet.addValue(rowIndex, columnIndex++, tag);
        }
        for (String attribute : record.getAttributes()) {
            tablet.addValue(rowIndex, columnIndex++, attribute);
        }
        int totalSize = columnNames.size();
        int fieldSize = record.getFields().size();
        int tagNAttributeSize = totalSize - fieldSize;
        for (int i = 0; i < fieldSize; i++) {
            Object fieldValue = record.getFields().get(i);
            switch (columnDataTypes.get(tagNAttributeSize + i)) {
                case INT32:
                    tablet.addValue(rowIndex, columnIndex++, (Integer) fieldValue);
                    break;
                case TIMESTAMP:
                case INT64:
                    tablet.addValue(rowIndex, columnIndex++, (Long) fieldValue);
                    break;
                case FLOAT:
                    tablet.addValue(rowIndex, columnIndex++, (Float) fieldValue);
                    break;
                case DOUBLE:
                    tablet.addValue(rowIndex, columnIndex++, (Double) fieldValue);
                    break;
                case BOOLEAN:
                    tablet.addValue(rowIndex, columnIndex++, (Boolean) fieldValue);
                    break;
                case TEXT:
                case STRING:
                    tablet.addValue(rowIndex, columnIndex++, (String) fieldValue);
                    break;
                case DATE:
                    tablet.addValue(rowIndex, columnIndex++, LocalDate.parse((String) fieldValue));
                    break;
                default:
                    throw new IotdbConnectorException(
                            CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unsupported data type: " + columnDataTypes.get(tagNAttributeSize + i));
            }
        }
    }

    public synchronized void close() throws IOException {
        try {
            flush();
        } finally {
            try {
                if (tableSession != null) {
                    tableSession.close();
                }
            } catch (IoTDBConnectionException e) {
                log.error("Close IoTDB client failed.", e);
            }
        }
    }

    synchronized void flush() {
        checkFlushException();
        if (batchList.isEmpty()) {
            return;
        }

        int maxRetries = sinkConfig.getMaxRetries();
        for (int i = 0; i <= maxRetries; i++) {
            try {
                for (Tablet tablet : batchList) {
                    tableSession.insert(tablet);
                }
                break;
            } catch (IoTDBConnectionException | StatementExecutionException e) {
                log.error("Writing records to IoTDB failed, retry times = {}", i, e);
                if (i >= sinkConfig.getMaxRetries()) {
                    throw new IotdbConnectorException(
                            CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                            "Writing records to IoTDB failed.",
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
                    throw new IotdbConnectorException(
                            CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                            "Unable to flush; interrupted while doing another attempt.",
                            e);
                }
            }
        }
        batchList.clear();
        tableNameList.clear();
        curBatchSize = 0;
    }

    private void checkFlushException() {
        if (flushException != null) {
            throw new IotdbConnectorException(
                    CommonErrorCodeDeprecated.FLUSH_DATA_FAILED,
                    "Writing records to IoTDB failed.",
                    flushException);
        }
    }

    private List<String> combineColumnNames(
            List<String> tagKeys, List<String> attributeKeys, List<String> fieldNames) {
        List<String> res = new ArrayList<>();
        res.addAll(tagKeys);
        res.addAll(attributeKeys);
        res.addAll(fieldNames);
        return res;
    }

    private List<ColumnCategory> generateColumnCategories(
            int tagSize, int attributeSize, int fieldSize) {
        List<ColumnCategory> res = new ArrayList<>();
        for (int i = 0; i < tagSize; ++i) {
            res.add(ColumnCategory.TAG);
        }
        for (int i = 0; i < attributeSize; ++i) {
            res.add(ColumnCategory.ATTRIBUTE);
        }
        for (int i = 0; i < fieldSize; ++i) {
            res.add(ColumnCategory.FIELD);
        }
        return res;
    }

    private List<TSDataType> generateColumnTypes(
            int tagSize, int attributeSize, List<TSDataType> fieldTypes) {
        List<TSDataType> res = new ArrayList<>();
        int s = tagSize + attributeSize;
        for (int i = 0; i < s; ++i) {
            res.add(TSDataType.STRING);
        }
        res.addAll(fieldTypes);
        return res;
    }
}
