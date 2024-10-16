/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.sls.serialization;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.format.text.exception.SeaTunnelTextFormatException;

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogContent;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.LogGroupData;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;

public class FastLogDeserializationSchema
        implements DeserializationSchema<SeaTunnelRow>, FastLogDeserialization<SeaTunnelRow> {

    public static final DateTimeFormatter TIME_FORMAT;
    private final CatalogTable catalogTable;

    static {
        TIME_FORMAT =
                (new DateTimeFormatterBuilder())
                        .appendPattern("HH:mm:ss")
                        .appendFraction(ChronoField.NANO_OF_SECOND, 0, 9, true)
                        .toFormatter();
    }

    public FastLogDeserializationSchema(CatalogTable catalogTable) {

        this.catalogTable = catalogTable;
    }

    @Override
    public SeaTunnelRow deserialize(byte[] bytes) throws IOException {
        return null;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return null;
    }

    public void deserialize(List<LogGroupData> logGroupDatas, Collector<SeaTunnelRow> out)
            throws IOException {
        for (LogGroupData logGroupData : logGroupDatas) {
            FastLogGroup logs = logGroupData.GetFastLogGroup();
            for (FastLog log : logs.getLogs()) {
                SeaTunnelRow seaTunnelRow = convertFastLogSchema(log);
                out.collect(seaTunnelRow);
            }
        }
    }

    private SeaTunnelRow convertFastLogSchema(FastLog log) {
        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
        List<Object> transformedRow = new ArrayList<>(rowType.getTotalFields());
        List<FastLogContent> logContents = log.getContents();
        for (FastLogContent flc : logContents) {
            int keyIndex = rowType.indexOf(flc.getKey(), false);
            if (keyIndex > -1) {
                Object field = convert(rowType.getFieldType(keyIndex), flc.getValue());
                transformedRow.add(keyIndex, field);
            }
        }
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(transformedRow.toArray());
        seaTunnelRow.setRowKind(RowKind.INSERT);
        seaTunnelRow.setTableId(catalogTable.getTableId().getTableName());
        return seaTunnelRow;
    }

    private Object convert(SeaTunnelDataType<?> fieldType, String field)
            throws SeaTunnelTextFormatException {
        switch (fieldType.getSqlType()) {
            case STRING:
                return field;
            case BOOLEAN:
                return Boolean.parseBoolean(field);
            case TINYINT:
                return Byte.parseByte(field);
            case SMALLINT:
                return Short.parseShort(field);
            case INT:
                return Integer.parseInt(field);
            case BIGINT:
                return Long.parseLong(field);
            case FLOAT:
                return Float.parseFloat(field);
            case DOUBLE:
                return Double.parseDouble(field);
            case DECIMAL:
                return new BigDecimal(field);
            case NULL:
                return null;
            case BYTES:
                return field.getBytes(StandardCharsets.UTF_8);
            default:
                throw new SeaTunnelTextFormatException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_DATA_TYPE,
                        String.format(
                                "SeaTunnel not support this data type [%s]",
                                fieldType.getSqlType()));
        }
    }
}
