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

package org.apache.seatunnel.connectors.seatunnel.kudu.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kudu.config.KuduSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.kudu.exception.KuduConnectorException;

import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.Operation;
import org.apache.kudu.client.PartialRow;

import java.time.LocalDateTime;

public class KuduRowSerializer implements SeaTunnelRowSerializer {

    private KuduTable kuduTable;
    private KuduSinkConfig.SaveMode saveMode;

    private SeaTunnelRowType seaTunnelRowType;

    public KuduRowSerializer(
            KuduTable kuduTable,
            KuduSinkConfig.SaveMode saveMode,
            SeaTunnelRowType seaTunnelRowType) {
        this.kuduTable = kuduTable;
        this.saveMode = saveMode;
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public Operation serializeRow(SeaTunnelRow row) {
        Operation operation;
        switch (row.getRowKind()) {
            case INSERT:
                if (saveMode == KuduSinkConfig.SaveMode.OVERWRITE) {
                    operation = kuduTable.newUpsert();
                    break;
                }
                operation = kuduTable.newInsert();
                break;
            case UPDATE_AFTER:
                operation = kuduTable.newUpsert();
                break;
            case DELETE:
                operation = kuduTable.newDelete();
                break;
            default:
                throw new KuduConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        "Unsupported write row kind: " + row.getRowKind());
        }
        transform(operation, row);
        return operation;
    }

    private void transform(Operation operation, SeaTunnelRow element) {
        PartialRow row = operation.getRow();
        for (int columnIndex = 0; columnIndex < seaTunnelRowType.getTotalFields(); columnIndex++) {
            SeaTunnelDataType<?> type = seaTunnelRowType.getFieldType(columnIndex);
            try {
                switch (type.getSqlType()) {
                    case BOOLEAN:
                    case TINYINT:
                    case SMALLINT:
                    case INT:
                    case BIGINT:
                    case FLOAT:
                    case DOUBLE:
                    case STRING:
                    case DECIMAL:
                    case BYTES:
                        row.addObject(
                                seaTunnelRowType.getFieldName(columnIndex),
                                element.getField(columnIndex));
                        break;
                    case TIMESTAMP:
                        Object fieldValue = element.getField(columnIndex);
                        if (fieldValue == null) {
                            row.addObject(seaTunnelRowType.getFieldName(columnIndex), null);
                        } else {
                            LocalDateTime localDateTime = (LocalDateTime) fieldValue;
                            row.addObject(
                                    seaTunnelRowType.getFieldName(columnIndex),
                                    java.sql.Timestamp.valueOf(localDateTime));
                        }
                        break;
                    default:
                        throw new KuduConnectorException(
                                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                "Unsupported column type: " + type.getSqlType());
                }
            } catch (ClassCastException e) {
                throw new KuduConnectorException(
                        KuduConnectorErrorCode.DATA_TYPE_CAST_FILED,
                        "Value type does not match column type "
                                + type.getSqlType()
                                + " for column "
                                + seaTunnelRowType.getFieldName(columnIndex));
            }
        }
    }
}
