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
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSinkConfig;

import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.Insert;
import org.apache.kudu.client.KuduClient;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;

/**
 * A Kudu outputFormat
 */
public class KuduOutputFormat
        implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(KuduOutputFormat.class);

    private String kuduMaster;
    private String kuduTableName;
    private KuduClient kuduClient;
    private KuduSession kuduSession;
    private KuduTable kuduTable;
    public static final long TIMEOUTMS = 18000;
    public static final long SESSIONTIMEOUTMS = 100000;
    public KuduOutputFormat(KuduSinkConfig kuduSinkConfig) {
        this.kuduMaster = kuduSinkConfig.getKuduMaster();
        this.kuduTableName = kuduSinkConfig.getKuduTableName();
        init();
    }

    public void write(SeaTunnelRow element) {

        Insert insert = kuduTable.newInsert();
        Schema schema = kuduTable.getSchema();

        int columnCount = schema.getColumnCount();
        PartialRow row = insert.getRow();
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
                        throw new IllegalArgumentException("Unsupported column type: " + col.getType());
                }
            } catch (ClassCastException e) {
                e.printStackTrace();
                throw new IllegalArgumentException(
                        "Value type does not match column type " + col.getType() +
                                " for column " + col.getName());
            }

        }

        try {
            kuduSession.apply(insert);
        } catch (KuduException e) {
            LOGGER.warn("kudu session insert data fail.", e);
            throw new RuntimeException("kudu session insert data fail.", e);
        }

    }

    public void init() {
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
            LOGGER.warn("Failed to initialize the Kudu client.", e);
            throw new RuntimeException("Failed to initialize the Kudu client.", e);
        }
        LOGGER.info("The Kudu client is successfully initialized", kuduMaster, kuduClient);
    }

    public void closeOutputFormat() {
        if (kuduClient != null) {
            try {
                kuduClient.close();
                kuduSession.close();
            } catch (KuduException e) {
                LOGGER.warn("Kudu Client close failed.", e);
            } finally {
                kuduClient = null;
                kuduSession = null;
            }
        }
    }
}
