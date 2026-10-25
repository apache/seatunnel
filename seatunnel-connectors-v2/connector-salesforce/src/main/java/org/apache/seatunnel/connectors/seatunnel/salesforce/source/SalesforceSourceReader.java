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

package org.apache.seatunnel.connectors.seatunnel.salesforce.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.salesforce.client.SalesforceClient;
import org.apache.seatunnel.connectors.seatunnel.salesforce.config.SalesforceParameters;
import org.apache.seatunnel.connectors.seatunnel.salesforce.config.SalesforceTableConfig;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Base64;
import java.util.List;

@Slf4j
public class SalesforceSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter DATETIME_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ");
    private static final DateTimeFormatter TIME_FMT =
            DateTimeFormatter.ofPattern("HH:mm:ss.SSS'Z'");

    private final SalesforceParameters params;
    private final List<SalesforceTableConfig> tableConfigs;
    private final SingleSplitReaderContext readerContext;
    private SalesforceClient client;

    SalesforceSourceReader(
            SalesforceParameters params,
            List<SalesforceTableConfig> tableConfigs,
            SingleSplitReaderContext readerContext) {
        this.params = params;
        this.tableConfigs = tableConfigs;
        this.readerContext = readerContext;
    }

    @Override
    public void open() throws Exception {
        client = new SalesforceClient(params);
        client.authenticate();
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    /**
     * Single-pass bounded read for the assigned split. For each configured table, submits one Bulk
     * API query job and hands the client a per-row consumer that converts the raw CSV Object[] to a
     * SeaTunnelRow, tags it with the table id, and forwards it to the downstream collector. Rows
     * flow through without buffering the whole result set in the reader. After every table has
     * drained, signals no-more-elements so the framework can close the split.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        try {
            for (SalesforceTableConfig tableConfig : tableConfigs) {
                CatalogTable catalogTable = tableConfig.getCatalogTable();
                SeaTunnelRowType rowType = catalogTable.getSeaTunnelRowType();
                int columnCount = rowType.getTotalFields();
                String tableId = tableConfig.getTableId();
                log.info("Reading rows from Salesforce object {}", tableConfig.getObjectName());
                client.executeBulkQuery(
                        tableConfig.getSoql(),
                        columnCount,
                        raw -> {
                            SeaTunnelRow row = convertRow(raw, rowType);
                            row.setTableId(tableId);
                            output.collect(row);
                        });
            }
        } finally {
            readerContext.signalNoMoreElement();
        }
    }

    private SeaTunnelRow convertRow(Object[] raw, SeaTunnelRowType rowType) {
        Object[] fields = new Object[raw.length];
        for (int i = 0; i < raw.length; i++) {
            String val = raw[i] == null ? null : raw[i].toString();
            if (val == null) {
                fields[i] = null;
                continue;
            }
            SeaTunnelDataType<?> type = rowType.getFieldType(i);
            fields[i] = parseValue(val, type);
        }
        return new SeaTunnelRow(fields);
    }

    private Object parseValue(String val, SeaTunnelDataType<?> type) {
        switch (type.getSqlType()) {
            case BOOLEAN:
                return Boolean.parseBoolean(val);
            case INT:
                return Integer.parseInt(val);
            case DOUBLE:
                return Double.parseDouble(val);
            case DATE:
                return LocalDate.parse(val, DATE_FMT);
            case TIMESTAMP:
                return LocalDateTime.parse(val, DATETIME_FMT);
            case TIME:
                return LocalTime.parse(val, TIME_FMT);
            case BYTES:
                return Base64.getDecoder().decode(val);
            default:
                return val;
        }
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {}
}
