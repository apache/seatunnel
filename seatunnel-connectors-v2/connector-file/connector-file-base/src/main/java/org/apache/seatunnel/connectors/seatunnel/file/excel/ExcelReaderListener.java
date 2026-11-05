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

package org.apache.seatunnel.connectors.seatunnel.file.excel;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import com.alibaba.excel.exception.ExcelDataConvertException;
import com.alibaba.excel.metadata.Cell;
import com.alibaba.excel.metadata.data.ReadCellData;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class ExcelReaderListener extends AnalysisEventListener<Map<Integer, Object>>
        implements Serializable, Closeable {
    private final String tableId;
    private final Collector<SeaTunnelRow> output;
    private int cellCount;

    private final ObjectMapper objectMapper = new ObjectMapper();

    protected Config pluginConfig;

    protected SeaTunnelRowType seaTunnelRowType;

    private SeaTunnelDataType<?>[] fieldTypes;
    private final int[] indexes;
    private final boolean useHeaderLine;
    // Maps output field indexes to Excel column indexes.
    private final Map<Integer, Integer> headerColumns = new HashMap<>();

    private ExcelCellUtils excelCellUtils;

    Map<Integer, String> customHeaders = new HashMap<>();

    public ExcelReaderListener(
            String tableId,
            Collector<SeaTunnelRow> output,
            ExcelCellUtils excelCellUtils,
            SeaTunnelRowType seaTunnelRowType) {
        this(tableId, output, excelCellUtils, seaTunnelRowType, null, true);
    }

    public ExcelReaderListener(
            String tableId,
            Collector<SeaTunnelRow> output,
            ExcelCellUtils excelCellUtils,
            SeaTunnelRowType seaTunnelRowType,
            int[] indexes) {
        this(tableId, output, excelCellUtils, seaTunnelRowType, indexes, true);
    }

    public ExcelReaderListener(
            String tableId,
            Collector<SeaTunnelRow> output,
            ExcelCellUtils excelCellUtils,
            SeaTunnelRowType seaTunnelRowType,
            int[] indexes,
            boolean useHeaderLine) {
        this.tableId = tableId;
        this.output = output;
        this.excelCellUtils = excelCellUtils;
        this.seaTunnelRowType = seaTunnelRowType;
        this.indexes = indexes;
        this.useHeaderLine = useHeaderLine;

        fieldTypes = seaTunnelRowType.getFieldTypes();
    }

    @Override
    public void invokeHead(Map<Integer, ReadCellData<?>> headMap, AnalysisContext context) {
        headerColumns.clear();
        customHeaders.clear();
        for (Map.Entry<Integer, ReadCellData<?>> entry : headMap.entrySet()) {
            String header = entry.getValue().getStringValue();
            if (StringUtils.isNotEmpty(header) && !"null".equals(header)) {
                customHeaders.put(entry.getKey(), header);
                // Schema fields may appear in a different order than the Excel columns.
                if (useHeaderLine) {
                    int fieldIndex = seaTunnelRowType.indexOf(header, false);
                    if (fieldIndex >= 0) {
                        headerColumns.put(fieldIndex, entry.getKey());
                    }
                }
            }
        }
        if (useHeaderLine) {
            for (int i = 0; i < fieldTypes.length; i++) {
                if (!headerColumns.containsKey(i)) {
                    log.warn(
                            "Excel header does not contain schema field '{}'; values will be null",
                            seaTunnelRowType.getFieldName(i));
                }
            }
        }
    }

    @Override
    public void invoke(Map<Integer, Object> data, AnalysisContext context) {
        cellCount = data.size();
        SeaTunnelRow seaTunnelRow = new SeaTunnelRow(fieldTypes.length);
        Map<Integer, Cell> cellMap = context.readRowHolder().getCellMap();
        int i = 0;
        for (; i < fieldTypes.length; i++) {
            Integer columnIndex;
            if (!useHeaderLine) {
                columnIndex = indexes == null ? i : indexes[i];
            } else {
                columnIndex = headerColumns.get(i);
            }
            Cell cell = columnIndex == null ? null : cellMap.get(columnIndex);
            if (cell == null) {
                seaTunnelRow.setField(i, null);
            } else {
                Object value = excelCellUtils.convert(data.get(columnIndex), fieldTypes[i], cell);
                seaTunnelRow.setField(i, value);
            }
        }
        seaTunnelRow.setTableId(tableId);
        output.collect(seaTunnelRow);
    }

    @Override
    public void doAfterAllAnalysed(AnalysisContext context) {
        log.info("excel parsing completed");
    }

    @Override
    public void onException(Exception exception, AnalysisContext context) {
        log.debug("cell parsing exception :{}", exception.getMessage());
        if (exception instanceof ExcelDataConvertException) {
            ExcelDataConvertException excelDataConvertException =
                    (ExcelDataConvertException) exception;
            log.debug(
                    "row:{},cell:{},data:{}",
                    excelDataConvertException.getRowIndex(),
                    excelDataConvertException.getColumnIndex(),
                    excelDataConvertException.getCellData());
        }
    }

    @Override
    public void close() throws IOException {}
}
