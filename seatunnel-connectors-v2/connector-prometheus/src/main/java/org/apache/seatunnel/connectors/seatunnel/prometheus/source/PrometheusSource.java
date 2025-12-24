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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSource;
import org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusQueryType;
import org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.prometheus.config.PrometheusSourceParameter;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class PrometheusSource extends HttpSource {

    private final PrometheusSourceParameter prometheusSourceParameter =
            new PrometheusSourceParameter();

    private final PrometheusQueryType queryType;

    protected PrometheusSource(ReadonlyConfig pluginConfig) {
        super(pluginConfig);
        queryType = pluginConfig.get(PrometheusSourceOptions.QUERY_TYPE);
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
}
