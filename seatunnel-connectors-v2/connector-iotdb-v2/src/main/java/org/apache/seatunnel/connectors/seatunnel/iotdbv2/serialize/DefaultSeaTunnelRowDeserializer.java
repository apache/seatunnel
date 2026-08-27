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

package org.apache.seatunnel.connectors.seatunnel.iotdbv2.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.constant.SourceConstants;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.exception.IotdbConnectorException;

import lombok.AllArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import shaded.org.apache.tsfile.enums.TSDataType;
import shaded.org.apache.tsfile.read.common.Field;
import shaded.org.apache.tsfile.read.common.RowRecord;

import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;

@Slf4j
@AllArgsConstructor
public class DefaultSeaTunnelRowDeserializer implements SeaTunnelRowDeserializer {

    private final SeaTunnelRowType rowType;

    private final String sqlDialect;

    @Override
    public SeaTunnelRow deserialize(RowRecord rowRecord) {
        if (SourceConstants.TABLE.equalsIgnoreCase(sqlDialect)) {
            return convertTableRow(rowRecord);
        }
        return convert(rowRecord);
    }

    private SeaTunnelRow convert(RowRecord rowRecord) {
        long timestamp = rowRecord.getTimestamp();
        List<Field> fields = rowRecord.getFields();
        if (fields.size() != (rowType.getTotalFields() - 1)) {
            throw new IotdbConnectorException(
                    CommonErrorCode.ILLEGAL_ARGUMENT, "Illegal SeaTunnelRowType: " + rowRecord);
        }
        Object[] seaTunnelFields = new Object[rowType.getTotalFields()];
        seaTunnelFields[0] = convertTimestamp(timestamp, rowType.getFieldType(0));
        for (int i = 1; i < rowType.getTotalFields(); i++) {
            Field field = fields.get(i - 1);
            if (field == null || field.getDataType() == null) {
                seaTunnelFields[i] = null;
                continue;
            }
            SeaTunnelDataType<?> seaTunnelFieldType = rowType.getFieldType(i);
            seaTunnelFields[i] = convert(seaTunnelFieldType, field);
        }
        return new SeaTunnelRow(seaTunnelFields);
    }

    private SeaTunnelRow convertTableRow(RowRecord rowRecord) {
        List<Field> fields = rowRecord.getFields();
        if (fields.size() != rowType.getTotalFields()) {
            throw new IotdbConnectorException(
                    CommonErrorCode.ILLEGAL_ARGUMENT, "Illegal SeaTunnelRowType: " + rowRecord);
        }
        Object[] seaTunnelFields = new Object[rowType.getTotalFields()];
        for (int i = 0; i < rowType.getTotalFields(); i++) {
            Field field = fields.get(i);
            if (field == null || field.getDataType() == null) {
                seaTunnelFields[i] = null;
                continue;
            }
            SeaTunnelDataType<?> seaTunnelFieldType = rowType.getFieldType(i);
            seaTunnelFields[i] = convert(seaTunnelFieldType, field);
        }
        return new SeaTunnelRow(seaTunnelFields);
    }

    private Object convert(SeaTunnelDataType<?> seaTunnelFieldType, Field field) {
        switch (field.getDataType()) {
            case INT32:
                Number int32 = field.getIntV();
                switch (seaTunnelFieldType.getSqlType()) {
                    case TINYINT:
                        return int32.byteValue();
                    case SMALLINT:
                        return int32.shortValue();
                    case INT:
                        return int32.intValue();
                    default:
                        throw new IotdbConnectorException(
                                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                "Unsupported data type: " + seaTunnelFieldType);
                }
            case INT64:
                return field.getLongV();
            case FLOAT:
                return field.getFloatV();
            case DOUBLE:
                return field.getDoubleV();
            case TEXT:
            case STRING:
                return field.getStringValue();
            case BOOLEAN:
                return field.getBoolV();
            case TIMESTAMP:
                long timestamp = (long) field.getObjectValue(TSDataType.TIMESTAMP);
                switch (seaTunnelFieldType.getSqlType()) {
                    case TIMESTAMP:
                        return new Date(timestamp)
                                .toInstant()
                                .atZone(ZoneOffset.UTC)
                                .toLocalDateTime();
                    case BIGINT:
                        return timestamp;
                    default:
                        throw new IotdbConnectorException(
                                CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                                "Unsupported data type: " + seaTunnelFieldType);
                }
            case DATE:
                return field.getObjectValue(TSDataType.DATE);
            case BLOB:
                return field.getStringValue();
            default:
                throw new IotdbConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported data type: " + field.getDataType());
        }
    }

    private Object convertTimestamp(long timestamp, SeaTunnelDataType<?> seaTunnelFieldType) {
        switch (seaTunnelFieldType.getSqlType()) {
            case TIMESTAMP:
                return new Date(timestamp).toInstant().atZone(ZoneOffset.UTC).toLocalDateTime();
            case BIGINT:
                return timestamp;
            default:
                throw new IotdbConnectorException(
                        CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                        "Unsupported data type: " + seaTunnelFieldType);
        }
    }
}
