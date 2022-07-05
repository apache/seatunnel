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


import org.apache.kudu.ColumnSchema;
import org.apache.kudu.Schema;
import org.apache.kudu.client.*;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSinkConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.sql.Timestamp;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A Kudu outputFormat
 */
public class KuduOutputFormat
        implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(KuduOutputFormat.class);

    private String kuduMaster;
    private String kuduTableName;
    private KuduClient kuduClient;
    private KuduSession kuduSession;
    private KuduTable kuduTable;


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

           // kuduClient.close();
           // kuduSession.close();
        } catch (KuduException e) {
            e.printStackTrace();
        }

    }

    public void init() {


        KuduClient.KuduClientBuilder kuduClientBuilder = new
                KuduClient.KuduClientBuilder(kuduMaster);
        kuduClientBuilder.defaultOperationTimeoutMs(1800000);

        this.kuduClient = kuduClientBuilder.build();
        this.kuduSession = kuduClient.newSession();
        this.kuduSession.setTimeoutMillis(100000);
        this.kuduSession.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC);

        try {
            this.kuduTable = kuduClient.openTable(kuduTableName);
        } catch (KuduException e) {
            e.printStackTrace();
        }


        logger.info("服务器地址#{}:客户端#{} 初始化成功...", kuduMaster, kuduClient);
    }



}
