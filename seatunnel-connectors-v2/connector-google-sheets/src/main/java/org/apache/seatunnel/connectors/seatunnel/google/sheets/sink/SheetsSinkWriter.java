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

package org.apache.seatunnel.connectors.seatunnel.google.sheets.sink;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.google.sheets.config.RangePosition;
import org.apache.seatunnel.connectors.seatunnel.google.sheets.config.SheetsParameters;
import org.apache.seatunnel.connectors.seatunnel.google.sheets.serialize.GoogleSheetsSerializer;
import org.apache.seatunnel.connectors.seatunnel.google.sheets.serialize.SeaTunnelRowSerializer;

import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.model.BatchUpdateValuesRequest;
import com.google.api.services.sheets.v4.model.ValueRange;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SheetsSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    private final SheetsParameters sheetsParameters;
    private final SeaTunnelRowSerializer seaTunnelRowSerializer;
    private final Sheets service;
    private final List<SeaTunnelRow> seaTunnelRowList;
    private final RangePosition rangePosition;
    private final Long targetRowCount;
    private final Integer batchSize = 100;
    private Long totalCount = 0L;

    public SheetsSinkWriter(SheetsParameters sheetsParameters, RangePosition rangePosition) throws IOException {
        this.sheetsParameters = sheetsParameters;
        this.seaTunnelRowSerializer = new GoogleSheetsSerializer();
        this.service = sheetsParameters.buildSheets();
        this.rangePosition = rangePosition;
        this.targetRowCount = rangePosition.getEndY() - rangePosition.getStartY() + 1;
        this.seaTunnelRowList = new ArrayList<>();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        seaTunnelRowList.add(element);
        totalCount++;
        if (totalCount % batchSize == 0 || totalCount >= targetRowCount) {
            flush();
        }
    }

    public void flush() throws IOException {
        List<List<Object>> values = seaTunnelRowSerializer.deserializeRow(seaTunnelRowList);
        List<ValueRange> data = new ArrayList<>();

        String start = rangePosition.getStartX();
        String end = rangePosition.getEndX() + (rangePosition.getStartY() + totalCount - 1);
        if (targetRowCount >= batchSize) {
            // If it is the last batch
            if (totalCount >= targetRowCount) {
                start += rangePosition.getEndY() - (totalCount % batchSize) + 1;
            } else {
                start += rangePosition.getStartY() + totalCount - batchSize;
            }
        } else {
            start += rangePosition.getStartY();
        }

        data.add(new ValueRange()
                .setRange(start + ":" + end)
                .setValues(values));
        BatchUpdateValuesRequest body = new BatchUpdateValuesRequest()
                .setValueInputOption("RAW")
                .setData(data);
        service.spreadsheets().values().batchUpdate(sheetsParameters.getSheetId(), body).execute();
        seaTunnelRowList.clear();
    }

    @Override
    public void close() throws IOException {
        // not need close
    }
}
