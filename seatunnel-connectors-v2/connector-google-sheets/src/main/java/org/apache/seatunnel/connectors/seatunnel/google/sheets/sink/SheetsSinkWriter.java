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
    private final Long rowCount;
    private final List<SeaTunnelRow> seaTunnelRowList;

    public SheetsSinkWriter(SheetsParameters sheetsParameters, Long rowCount) throws IOException {
        this.sheetsParameters = sheetsParameters;
        this.seaTunnelRowSerializer = new GoogleSheetsSerializer();
        this.service = sheetsParameters.buildSheets();
        this.rowCount = rowCount;
        this.seaTunnelRowList = new ArrayList<>();
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        if (this.seaTunnelRowList.size() < rowCount - 1) {
            this.seaTunnelRowList.add(element);
            return;
        }
        this.seaTunnelRowList.add(element);
        List<List<Object>> values = this.seaTunnelRowSerializer.deserializeRow(this.seaTunnelRowList);
        List<ValueRange> data = new ArrayList<>();
        data.add(new ValueRange()
                .setRange(this.sheetsParameters.getRange())
                .setValues(values));
        BatchUpdateValuesRequest body = new BatchUpdateValuesRequest()
                .setValueInputOption("RAW")
                .setData(data);
        service.spreadsheets().values().batchUpdate(this.sheetsParameters.getSheetId(), body).execute();
    }

    @Override
    public void close() throws IOException {
        // not need close
    }
}
