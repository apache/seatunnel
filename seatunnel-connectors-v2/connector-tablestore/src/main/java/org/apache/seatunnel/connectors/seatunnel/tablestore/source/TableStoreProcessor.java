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
package org.apache.seatunnel.connectors.seatunnel.tablestore.source;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.tablestore.serialize.SeaTunnelRowDeserializer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alicloud.openservices.tablestore.model.StreamRecord;
import com.alicloud.openservices.tablestore.tunnel.worker.IChannelProcessor;
import com.alicloud.openservices.tablestore.tunnel.worker.ProcessRecordsInput;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class TableStoreProcessor implements IChannelProcessor {
    private String tableName = null;
    private String primaryKey = null;
    private Collector<SeaTunnelRow> output = null;
    protected SeaTunnelRowDeserializer seaTunnelRowDeserializer;
    private static final Logger log = LoggerFactory.getLogger(TableStoreProcessor.class);

    public TableStoreProcessor(
            String tableName, String primaryKey, Collector<SeaTunnelRow> output) {
        this.tableName = tableName;
        this.primaryKey = primaryKey;
        this.output = output;
    }

    @Override
    public void process(ProcessRecordsInput input) {
        log.info("Default record processor, would print records count");

        log.info(
                String.format(
                        "Process %d records, NextToken: %s",
                        input.getRecords().size(), input.getNextToken()));

        for (StreamRecord r : input.getRecords()) {
            try {
                List<Object> fields = new ArrayList<>();
                Arrays.stream(r.getPrimaryKey().getPrimaryKeyColumns())
                        .forEach(
                                k -> {
                                    fields.add(k.getValue().toString());
                                });
                r.getColumns()
                        .forEach(
                                k -> {
                                    fields.add(k.getColumn().getValue().toString());
                                });
                SeaTunnelRow row = new SeaTunnelRow(fields.toArray());
                row.setTableId(tableName);
                switch ((r.getRecordType())) {
                    case PUT:
                        row.setRowKind(RowKind.INSERT);
                        break;
                    case UPDATE:
                        row.setRowKind(RowKind.UPDATE_AFTER);
                        break;
                    case DELETE:
                        row.setRowKind(RowKind.DELETE);
                        break;
                }
                output.collect(row);
            } catch (Exception e) {
                log.error("send to target failed with record: " + r.toString(), e);
            }
        }
    }

    @Override
    public void shutdown() {
        log.info("process shutdown du to finished for table: " + tableName);
    }
}
