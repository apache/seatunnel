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

    private ExcelCellUtils excelCellUtils;

    Map<Integer, String> customHeaders = new HashMap<>();

    public ExcelReaderListener(
            String tableId,
            Collector<SeaTunnelRow> output,
            ExcelCellUtils excelCellUtils,
            SeaTunnelRowType seaTunnelRowType) {
        this.tableId = tableId;
        this.output = output;
        this.excelCellUtils = excelCellUtils;
        this.seaTunnelRowType = seaTunnelRowType;

        fieldTypes = seaTunnelRowType.getFieldTypes();
    }

    @Override
    public void invokeHead(Map<Integer, ReadCellData<?>> headMap, AnalysisContext context) {
        for (int i = 0; i < headMap.size(); i++) {
            String header = headMap.get(i).getStringValue();
            if (!"null".equals(header)) {
                customHeaders.put(i, header);
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
            if (cellMap.get(i) == null) {
                seaTunnelRow.setField(i, null);
            } else {
                Object cell = excelCellUtils.convert(data.get(i), fieldTypes[i], cellMap.get(i));
                seaTunnelRow.setField(i, cell);
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
