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

package org.apache.seatunnel.connectors.seatunnel.console.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.event.SchemaChangeEvent;
import org.apache.seatunnel.api.table.event.handler.DataTypeChangeEventDispatcher;
import org.apache.seatunnel.api.table.event.handler.DataTypeChangeEventHandler;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import org.apache.commons.lang3.StringUtils;

import lombok.extern.slf4j.Slf4j;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class ConsoleSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private SeaTunnelRowType seaTunnelRowType;
    private final AtomicLong rowCounter = new AtomicLong(0);
    private final SinkWriter.Context context;
    private final DataTypeChangeEventHandler dataTypeChangeEventHandler;

    boolean isPrintData = true;
    int delayMs = 0;

    public ConsoleSinkWriter(
            SeaTunnelRowType seaTunnelRowType,
            SinkWriter.Context context,
            boolean isPrintData,
            int delayMs) {
        this.seaTunnelRowType = seaTunnelRowType;
        this.context = context;
        this.isPrintData = isPrintData;
        this.delayMs = delayMs;
        this.dataTypeChangeEventHandler = new DataTypeChangeEventDispatcher();
        log.info("output rowType: {}", fieldsInfo(seaTunnelRowType));
    }

    @Override
    public void applySchemaChange(SchemaChangeEvent event) {
        log.info("changed rowType before: {}", fieldsInfo(seaTunnelRowType));
        seaTunnelRowType = dataTypeChangeEventHandler.reset(seaTunnelRowType).apply(event);
        log.info("changed rowType after: {}", fieldsInfo(seaTunnelRowType));
    }

    @Override
    public void write(SeaTunnelRow element) {
        String[] arr = new String[seaTunnelRowType.getTotalFields()];
        SeaTunnelDataType<?>[] fieldTypes = seaTunnelRowType.getFieldTypes();
        Object[] fields = element.getFields();
        for (int i = 0; i < fieldTypes.length; i++) {
            arr[i] = fieldToString(fieldTypes[i], fields[i]);
        }
        if (isPrintData) {
            log.info(
                    "subtaskIndex={}  rowIndex={}:  SeaTunnelRow#tableId={} SeaTunnelRow#kind={} : {}",
                    context.getIndexOfSubtask(),
                    rowCounter.incrementAndGet(),
                    element.getTableId(),
                    element.getRowKind(),
                    StringUtils.join(arr, ", "));
        }
        if (delayMs > 0) {
            try {
                Thread.sleep(delayMs);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new SeaTunnelException(e);
            }
        }
    }

    @Override
    public void close() {
        // nothing
    }

    private String fieldsInfo(SeaTunnelRowType seaTunnelRowType) {
        String[] fieldsInfo = new String[seaTunnelRowType.getTotalFields()];
        for (int i = 0; i < seaTunnelRowType.getTotalFields(); i++) {
            fieldsInfo[i] =
                    String.format(
                            "%s<%s>",
                            seaTunnelRowType.getFieldName(i), seaTunnelRowType.getFieldType(i));
        }
        return StringUtils.join(fieldsInfo, ", ");
    }

    private String fieldToString(SeaTunnelDataType<?> type, Object value) {
        if (value == null) {
            return null;
        }
        switch (type.getSqlType()) {
            case ARRAY:
            case BYTES:
                List<String> arrayData = new ArrayList<>();
                for (int i = 0; i < Array.getLength(value); i++) {
                    arrayData.add(String.valueOf(Array.get(value, i)));
                }
                return arrayData.toString();
            case MAP:
                return JsonUtils.toJsonString(value);
            case ROW:
                List<String> rowData = new ArrayList<>();
                SeaTunnelRowType rowType = (SeaTunnelRowType) type;
                for (int i = 0; i < rowType.getTotalFields(); i++) {
                    rowData.add(
                            fieldToString(
                                    rowType.getFieldTypes()[i],
                                    ((SeaTunnelRow) value).getField(i)));
                }
                return rowData.toString();
            default:
                return String.valueOf(value);
        }
    }
}
