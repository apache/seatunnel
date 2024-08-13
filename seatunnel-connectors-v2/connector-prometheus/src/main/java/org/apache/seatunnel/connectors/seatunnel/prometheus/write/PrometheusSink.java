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
package org.apache.seatunnel.connectors.seatunnel.prometheus.write;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

public class PrometheusSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink {

    protected final HttpParameter httpParameter = new HttpParameter();
    protected SeaTunnelRowType seaTunnelRowType;
    protected Config pluginConfig;

    public PrometheusSink(Config pluginConfig, SeaTunnelRowType rowType) {
        this.pluginConfig = pluginConfig;
        CheckResult result = CheckConfigUtil.checkAllExists(pluginConfig, HttpConfig.URL.key());
        if (!result.isSuccess()) {
            throw new HttpConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, result.getMsg()));
        }
        httpParameter.setUrl(pluginConfig.getString(HttpConfig.URL.key()));
        if (pluginConfig.hasPath(HttpConfig.HEADERS.key())) {
            httpParameter.setHeaders(
                    pluginConfig.getConfig(HttpConfig.HEADERS.key()).entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry -> String.valueOf(entry.getValue().unwrapped()),
                                            (v1, v2) -> v2)));
        }
        if (pluginConfig.hasPath(HttpConfig.PARAMS.key())) {
            httpParameter.setHeaders(
                    pluginConfig.getConfig(HttpConfig.PARAMS.key()).entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry -> String.valueOf(entry.getValue().unwrapped()),
                                            (v1, v2) -> v2)));
        }
        this.seaTunnelRowType = rowType;

        if (Objects.isNull(httpParameter.getHeaders())) {
            Map<String, String> headers = new HashMap<>();
            headers.put("Content-type", "application/x-protobuf");
            headers.put("Content-Encoding", "snappy");
            headers.put("X-Prometheus-Remote-Write-Version", "0.1.0");
            httpParameter.setHeaders(headers);
        } else {
            httpParameter.getHeaders().put("Content-type", "application/x-protobuf");
            httpParameter.getHeaders().put("Content-Encoding", "snappy");
            httpParameter.getHeaders().put("X-Prometheus-Remote-Write-Version", "0.1.0");
        }
    }

    @Override
    public String getPluginName() {
        return "Prometheus";
    }

    @Override
    public PrometheusWriter createWriter(SinkWriter.Context context) {
        return new PrometheusWriter(seaTunnelRowType, httpParameter, pluginConfig);
    }
}
