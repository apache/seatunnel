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

import com.aliyun.openservices.log.common.FastLog;
import com.aliyun.openservices.log.common.FastLogGroup;
import com.aliyun.openservices.log.common.LogGroupData;

import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;

public class FastLogDeserializationContent
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

    public FastLogDeserializationContent(CatalogTable catalogTable) {
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
                SeaTunnelRow seaTunnelRow = convertFastLogContent(log);
                out.collect(seaTunnelRow);
            }
        }
    }

    private SeaTunnelRow convertFastLogContent(FastLog log) {
        SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
        List<Object> transformedRow = new ArrayList<>(rowType.getTotalFields());
        // json format
        StringBuilder jsonStringBuilder = new StringBuilder();
        jsonStringBuilder.append("{");
        log.getContents()
                .forEach(
                        (content) ->
                                jsonStringBuilder
                                        .append("\"")
                                        .append(content.getKey())
                                        .append("\":\"")
                                        .append(content.getValue())
                                        .append("\","));
        jsonStringBuilder.deleteCharAt(jsonStringBuilder.length() - 1); // 删除最后一个逗号
        jsonStringBuilder.append("}");
        // content field
        transformedRow.add(jsonStringBuilder.toString());
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(transformedRow.toArray());
        seaTunnelRow.setRowKind(RowKind.INSERT);
        seaTunnelRow.setTableId(catalogTable.getTableId().getTableName());
        return seaTunnelRow;
    }
}
