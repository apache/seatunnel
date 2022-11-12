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

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.google.sheets.config.SheetsConfig;
import org.apache.seatunnel.connectors.seatunnel.google.sheets.config.SheetsParameters;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@AutoService(SeaTunnelSink.class)
public class SheetsSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private SheetsParameters sheetsParameters;
    private SeaTunnelRowType seaTunnelRowType;
    private Long rowCount;

    @Override
    public String getPluginName() {
        return "GoogleSheets";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult checkResult = CheckConfigUtil.checkAllExists(pluginConfig, SheetsConfig.SERVICE_ACCOUNT_KEY.key(), SheetsConfig.SHEET_ID.key(), SheetsConfig.SHEET_NAME.key(), SheetsConfig.RANGE.key());
        if (!checkResult.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, checkResult.getMsg());
        }
        this.sheetsParameters = new SheetsParameters().buildWithConfig(pluginConfig);
        this.rowCount = matchRowCount(this.sheetsParameters.getRange());
    }

    @Override
    public void setTypeInfo(SeaTunnelRowType seaTunnelRowType) {
        this.seaTunnelRowType = seaTunnelRowType;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getConsumedType() {
        return seaTunnelRowType;
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context) throws IOException {
        return new SheetsSinkWriter(this.sheetsParameters, this.rowCount);
    }

    // Match the number of inserted rows from the range
    private Long matchRowCount(String range) {
        Pattern p = Pattern.compile("[0-9]+");
        Matcher matcher = p.matcher(range);
        long start = 0L;
        long end = 0L;
        if (matcher.find()) {
            start = Long.parseLong(matcher.group());
        }
        if (matcher.find()) {
            end = Long.parseLong(matcher.group());
        }
        return end - start + 1;
    }
}
