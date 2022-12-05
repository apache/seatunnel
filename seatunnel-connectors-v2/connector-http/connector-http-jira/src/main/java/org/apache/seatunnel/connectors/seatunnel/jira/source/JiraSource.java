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

package org.apache.seatunnel.connectors.seatunnel.jira.source;

import static org.apache.seatunnel.connectors.seatunnel.http.util.AuthorizationUtil.getTokenByBasicAuth;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSource;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;
import org.apache.seatunnel.connectors.seatunnel.jira.source.config.JiraSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.jira.source.config.JiraSourceParameter;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@AutoService(SeaTunnelSource.class)
public class JiraSource extends HttpSource {
    private final JiraSourceParameter jiraSourceParameter = new JiraSourceParameter();

    @Override
    public String getPluginName() {
        return "Jira";
    }

    @Override
    public Boundedness getBoundedness() {
        if (JobMode.BATCH.equals(jobContext.getJobMode())) {
            return Boundedness.BOUNDED;
        }
        throw new UnsupportedOperationException("Jira source connector not support unbounded operation");
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, JiraSourceConfig.URL.key(), JiraSourceConfig.EMAIL.key(), JiraSourceConfig.API_TOKEN.key());
        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }
        //get accessToken by basic auth
        String accessToken = getTokenByBasicAuth(pluginConfig.getString(JiraSourceConfig.EMAIL.key()), pluginConfig.getString(JiraSourceConfig.API_TOKEN.key()));
        jiraSourceParameter.buildWithConfig(pluginConfig, accessToken);
        buildSchemaWithConfig(pluginConfig);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(SingleSplitReaderContext readerContext) throws Exception {
        return new HttpSourceReader(this.jiraSourceParameter, readerContext, this.deserializationSchema, jsonField, contentField);
    }
}
