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
package org.apache.seatunnel.connectors.seatunnel.prometheus.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpConfig;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class PrometheusSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink {

    protected final HttpParameter httpParameter = new HttpParameter();
    protected SeaTunnelRowType seaTunnelRowType;
    protected ReadonlyConfig pluginConfig;

    public PrometheusSink(ReadonlyConfig pluginConfig, SeaTunnelRowType rowType) {
        this.pluginConfig = pluginConfig;
        httpParameter.setUrl(pluginConfig.get(HttpConfig.URL));
        if (pluginConfig.getOptional(HttpConfig.HEADERS).isPresent()) {
            httpParameter.setHeaders(pluginConfig.get(HttpConfig.HEADERS));
        }
        if (pluginConfig.getOptional(HttpConfig.PARAMS).isPresent()) {
            httpParameter.setHeaders(pluginConfig.get(HttpConfig.PARAMS));
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
