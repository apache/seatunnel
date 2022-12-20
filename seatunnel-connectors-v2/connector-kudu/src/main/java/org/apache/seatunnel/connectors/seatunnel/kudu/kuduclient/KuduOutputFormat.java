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

package org.apache.seatunnel.connectors.seatunnel.kudu.kuduclient;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorException;

import lombok.extern.slf4j.Slf4j;
import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.client.Upsert;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;

/**
 * A Kudu outputFormat
 */
@Slf4j
public class KuduOutputFormat
        implements Serializable {

    public static final long TIMEOUTMS = 18000;
    public static final long SESSIONTIMEOUTMS = 100000;

    private final String kuduMaster;
    private final String kuduTableName;
    private final KuduSinkConfig.SaveMode saveMode;
    private KuduClient kuduClient;
    private KuduSession kuduSession;
    private KuduTable kuduTable;

    public KuduOutputFormat(KuduSinkConfig kuduSinkConfig) {
        this.kuduMaster = kuduSinkConfig.getKuduMaster();
        this.kuduTableName = kuduSinkConfig.getKuduTableName();
        this.saveMode = kuduSinkConfig.getSaveMode();
        init();
    }

    private void transform(PartialRow row, SeaTunnelRow element, Schema schema) {
        int columnCount = schema.getColumnCount();
        for (int columnIndex = 0; columnIndex < columnCount; columnIndex++) {
            ColumnSchema col = schema.getColumnByIndex(columnIndex);
            try {
                switch (col.getType()) {
                    case BOOL:
                        row.addBoolean(columnIndex, (Boolean) element.getField(columnIndex));
                        break;
                    case INT8:
                        row.addByte(columnIndex, (Byte) element.getField(columnIndex));
                        break;
                    case INT16:
                        row.addShort(columnIndex, (Short) element.getField(columnIndex));
                        break;
                    case INT32:
                        row.addInt(columnIndex, (Integer) element.getField(columnIndex));
                        break;
                    case INT64:
                        row.addLong(columnIndex, (Long) element.getField(columnIndex));
                        break;
                    case UNIXTIME_MICROS:
                        if (element.getField(columnIndex) instanceof Timestamp) {
                            row.addTimestamp(columnIndex, (Timestamp) element.getField(columnIndex));
                        } else {
                            row.addLong(columnIndex, (Long) element.getField(columnIndex));
                        }
                        break;
                    case FLOAT:
                        row.addFloat(columnIndex, (Float) element.getField(columnIndex));
                        break;
                    case DOUBLE:
                        row.addDouble(columnIndex, (Double) element.getField(columnIndex));
                        break;
                    case STRING:
                        row.addString(columnIndex, element.getField(columnIndex).toString());
                        break;
                    case BINARY:
                        if (element.getField(columnIndex) instanceof byte[]) {
                            row.addBinary(columnIndex, (byte[]) element.getField(columnIndex));
                        } else {
                            row.addBinary(columnIndex, (ByteBuffer) element.getField(columnIndex));
                        }
                        break;
                    case DECIMAL:
                        row.addDecimal(columnIndex, (BigDecimal) element.getField(columnIndex));
                        break;
                    default:
                        throw new KuduConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE, "Unsupported column type: " + col.getType());
                }
            } catch (ClassCastException e) {
                throw new KuduConnectorException(KuduConnectorErrorCode.DATA_TYPE_CAST_FILED,
                        "Value type does not match column type " + col.getType() +
                                " for column " + col.getName());
            }

        }
    }

    private void upsert(SeaTunnelRow element) {
        Upsert upsert = kuduTable.newUpsert();
        Schema schema = kuduTable.getSchema();
        PartialRow row = upsert.getRow();
        transform(row, element, schema);
        try {
            kuduSession.apply(upsert);
        } catch (KuduException e) {
            throw new KuduConnectorException(KuduConnectorErrorCode.KUDU_UPSERT_FAILED, e);
        }
    }

    private void insert(SeaTunnelRow element) {
        Insert insert = kuduTable.newInsert();
        Schema schema = kuduTable.getSchema();
        PartialRow row = insert.getRow();
        transform(row, element, schema);
        try {
            kuduSession.apply(insert);
        } catch (KuduException e) {
            throw new KuduConnectorException(KuduConnectorErrorCode.KUDU_INSERT_FAILED, e);
        }
    }

    public void write(SeaTunnelRow element) {
        switch (saveMode) {
            case APPEND:
                insert(element);
                break;
            case OVERWRITE:
                upsert(element);
                break;
            default:
                throw new KuduConnectorException(CommonErrorCode.FLUSH_DATA_FAILED, String.format("Unsupported saveMode: %s.", saveMode.name()));
        }
    }

    private void init() {
        KuduClient.KuduClientBuilder kuduClientBuilder = new
                KuduClient.KuduClientBuilder(kuduMaster);
        kuduClientBuilder.defaultOperationTimeoutMs(TIMEOUTMS);
        this.kuduClient = kuduClientBuilder.build();
        this.kuduSession = kuduClient.newSession();
        this.kuduSession.setTimeoutMillis(SESSIONTIMEOUTMS);
        this.kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);
        try {
            kuduTable = kuduClient.openTable(kuduTableName);
        } catch (KuduException e) {
            throw new KuduConnectorException(KuduConnectorErrorCode.INIT_KUDU_CLIENT_FAILED, e);
        }
        log.info("The Kudu client for Master: {} is initialized successfully.", kuduMaster);
    }

    public void closeOutputFormat() {
        if (kuduClient != null) {
            try {
                kuduClient.close();
                kuduSession.close();
            } catch (KuduException ignored) {
                log.warn("Failed to close Kudu Client.", ignored);
            } finally {
                kuduClient = null;
                kuduSession = null;
            }
        }
    }
}
