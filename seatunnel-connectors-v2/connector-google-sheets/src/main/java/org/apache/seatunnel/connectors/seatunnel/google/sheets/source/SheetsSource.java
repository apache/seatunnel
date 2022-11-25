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

package org.apache.seatunnel.connectors.seatunnel.google.sheets.source;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.google.sheets.config.SheetsConfig;
import org.apache.seatunnel.connectors.seatunnel.google.sheets.config.SheetsParameters;
import org.apache.seatunnel.connectors.seatunnel.google.sheets.exception.GoogleSheetsConnectorException;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

@AutoService(SeaTunnelSource.class)
public class SheetsSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private SeaTunnelRowType seaTunnelRowType;

    private SheetsParameters sheetsParameters;

    private DeserializationSchema<SeaTunnelRow> deserializationSchema;

    @Override
    public String getPluginName() {
        return "GoogleSheets";
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult checkResult = CheckConfigUtil.checkAllExists(pluginConfig, SheetsConfig.SERVICE_ACCOUNT_KEY.key(), SheetsConfig.SHEET_ID.key(), SheetsConfig.SHEET_NAME.key(), SheetsConfig.RANGE.key(), SeaTunnelSchema.SCHEMA.key());
        if (!checkResult.isSuccess()) {
            throw new GoogleSheetsConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                String.format("PluginName: %s, PluginType: %s, Message: %s",
                    getPluginName(), PluginType.SOURCE, checkResult.getMsg()));
        }
        this.sheetsParameters = new SheetsParameters().buildWithConfig(pluginConfig);
        if (pluginConfig.hasPath(SeaTunnelSchema.SCHEMA.key())) {
            Config schema = pluginConfig.getConfig(SeaTunnelSchema.SCHEMA.key());
            this.seaTunnelRowType = SeaTunnelSchema.buildWithConfig(schema).getSeaTunnelRowType();
        } else {
            this.seaTunnelRowType = SeaTunnelSchema.buildSimpleTextSchema();
        }
        this.deserializationSchema = new JsonDeserializationSchema(false, false, seaTunnelRowType);
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return seaTunnelRowType;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext) throws Exception {
        return new SheetsSourceReader(sheetsParameters, readerContext, deserializationSchema, this.seaTunnelRowType);
    }
}
