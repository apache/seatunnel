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

package org.apache.seatunnel.connectors.seatunnel.prometheus.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSource;
import org.apache.seatunnel.connectors.seatunnel.prometheus.Exception.PrometheusConnectorException;
import org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceParameter;

import lombok.extern.slf4j.Slf4j;

import static org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceConfig.INSTANT_QUERY;
import static org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceConfig.QUERY_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceConfig.RANGE_QUERY;

@Slf4j
public class PrometheusSource extends HttpSource {

    private final PrometheusSourceParameter prometheusSourceParameter =
            new PrometheusSourceParameter();

    private final String queryType;

    protected PrometheusSource(Config pluginConfig) {
        super(pluginConfig);
        queryType =
                pluginConfig.hasPath(QUERY_TYPE.key())
                        ? pluginConfig.getString(QUERY_TYPE.key())
                        : QUERY_TYPE.defaultValue();
        CheckResult result = checkResult(queryType, pluginConfig);

        if (!result.isSuccess()) {
            throw new PrepareFailException(getPluginName(), PluginType.SOURCE, result.getMsg());
        }
        prometheusSourceParameter.buildWithConfig(pluginConfig);
    }

    @Override
    public String getPluginName() {
        return "Prometheus";
    }

    @Override
    public Boundedness getBoundedness() {
        return JobMode.BATCH.equals(jobContext.getJobMode())
                ? Boundedness.BOUNDED
                : Boundedness.UNBOUNDED;
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new PrometheusSourceReader(
                this.prometheusSourceParameter, readerContext, contentField, queryType);
    }

    private CheckResult checkResult(String queryType, Config pluginConfig) {
        switch (queryType) {
            case RANGE_QUERY:
                return CheckConfigUtil.checkAllExists(
                        pluginConfig,
                        PrometheusSourceConfig.QUERY.key(),
                        PrometheusSourceConfig.RangeConfig.START.key(),
                        PrometheusSourceConfig.RangeConfig.END.key(),
                        PrometheusSourceConfig.RangeConfig.STEP.key());
            case INSTANT_QUERY:
                return CheckConfigUtil.checkAllExists(
                        pluginConfig, PrometheusSourceConfig.QUERY.key());
            default:
                throw new PrometheusConnectorException(
                        CommonErrorCode.UNSUPPORTED_METHOD, "unsupported query type");
        }
    }
}
