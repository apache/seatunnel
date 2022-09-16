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

package org.apache.seatunnel.connectors.seatunnel.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.DingTalkConstant;
import org.apache.seatunnel.connectors.seatunnel.common.DingTalkParameter;
import org.apache.seatunnel.connectors.seatunnel.common.DingTalkUtil;
import org.apache.seatunnel.connectors.seatunnel.common.schema.SeaTunnelSchema;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;

@AutoService(SeaTunnelSource.class)
public class DingTalkSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    protected final DingTalkParameter dtParameter = new DingTalkParameter();
    protected SeaTunnelRowType rowType;
    protected JobContext jobContext;
    protected DeserializationSchema<SeaTunnelRow> deserializationSchema;

    @Override
    public String getPluginName() {
        return "DingTalk";
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode()) ? Boundedness.BOUNDED : Boundedness.UNBOUNDED;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult hasClient = CheckConfigUtil.checkAllExists(pluginConfig, DingTalkConstant.API_CLIENT);
        if (!hasClient.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, hasClient.getMsg());
        }
        CheckResult hasToken = CheckConfigUtil.checkAllExists(pluginConfig, DingTalkConstant.ACCESS_TOKEN);
        if (!hasToken.isSuccess()) {
            CheckResult hasKey = CheckConfigUtil.checkAllExists(pluginConfig, DingTalkConstant.APP_KEY, DingTalkConstant.APP_SECRET);
            if (!hasKey.isSuccess()) {
                throw new PrepareFailException(getPluginName(), PluginType.SOURCE, hasKey.getMsg());
            }
            String appToken = DingTalkUtil.getAppToken(pluginConfig.getString(DingTalkConstant.APP_KEY), pluginConfig.getString(DingTalkConstant.APP_SECRET));
            if (null == appToken) {
                throw new PrepareFailException(getPluginName(), PluginType.SOURCE, "Get App Token Error!");
            }
            this.dtParameter.setAccessToken(appToken);
        }
        this.dtParameter.buildParameter(pluginConfig, hasToken.isSuccess());

        if (pluginConfig.hasPath(DingTalkConstant.SCHEMA)) {
            Config schema = pluginConfig.getConfig(DingTalkConstant.SCHEMA);
            this.rowType = SeaTunnelSchema.buildWithConfig(schema).getSeaTunnelRowType();
        } else {
            this.rowType = SeaTunnelSchema.buildSimpleTextSchema();
        }
    }

    @Override
    public void setJobContext(JobContext jobContext) {
        this.jobContext = jobContext;
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.rowType;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext) throws Exception {
        return new DingTalkSourceReader(this.dtParameter, readerContext, this.deserializationSchema);
    }

}
