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

package org.apache.seatunnel.connectors.seatunnel.iotdb.serialize;

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iotdb.exception.IotdbConnectorException;

import lombok.AllArgsConstructor;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import java.time.ZoneOffset;
import java.util.Date;
import java.util.List;

@AllArgsConstructor
public class DefaultSeaTunnelRowDeserializer implements SeaTunnelRowDeserializer {

    private final SeaTunnelRowType rowType;

    @Override
    public SeaTunnelRow deserialize(RowRecord rowRecord) {
        return convert(rowRecord);
    }

    private SeaTunnelRow convert(RowRecord rowRecord) {
        long timestamp = rowRecord.getTimestamp();
        List<Field> fields = rowRecord.getFields();
        if (fields.size() != (rowType.getTotalFields() - 1)) {
            throw new IotdbConnectorException(CommonErrorCode.ILLEGAL_ARGUMENT,
                "Illegal SeaTunnelRowType: " + rowRecord);
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

    private Object convert(SeaTunnelDataType<?> seaTunnelFieldType,
                           Field field) {
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
                        throw new IotdbConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                            "Unsupported data type: " + seaTunnelFieldType);
                }
            case INT64:
                return field.getLongV();
            case FLOAT:
                return field.getFloatV();
            case DOUBLE:
                return field.getDoubleV();
            case TEXT:
                return field.getStringValue();
            case BOOLEAN:
                return field.getBoolV();
            default:
                throw new IotdbConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "Unsupported data type: " + field.getDataType());
        }
    }

    private Object convertTimestamp(long timestamp,
                                    SeaTunnelDataType<?> seaTunnelFieldType) {
        switch (seaTunnelFieldType.getSqlType()) {
            case TIMESTAMP:
                return new Date(timestamp)
                    .toInstant()
                    .atZone(ZoneOffset.UTC)
                    .toLocalDateTime();
            case BIGINT:
                return timestamp;
            default:
                throw new IotdbConnectorException(CommonErrorCode.UNSUPPORTED_DATA_TYPE,
                    "Unsupported data type: " + seaTunnelFieldType);
        }
    }
}
