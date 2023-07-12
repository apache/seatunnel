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

package org.apache.seatunnel.engine.server.telemetry.metrics;

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.TelemetryMetricConfig;

import com.hazelcast.instance.impl.Node;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

import java.io.IOException;
import org.apache.seatunnel.engine.server.telemetry.metrics.exports.JobMetricExports;
import org.apache.seatunnel.engine.server.telemetry.metrics.exports.ThreadPoolStatusExports;

public class ExportsInstance {

    private Node node;
    private TelemetryMetricConfig metricConfig;

    public ExportsInstance(Node node, SeaTunnelConfig seaTunnelConfig) throws IOException {
        this.node = node;
        this.metricConfig = seaTunnelConfig.getEngineConfig().getTelemetryConfig().getMetric();
        start();
    }

    private void start() throws IOException {
        if(metricConfig.isLoadDefaultExports()){
            DefaultExports.initialize();
        }
        HTTPServer httpServer =
                new HTTPServer.Builder().withPort(metricConfig.getHttpPort()).build();
        CollectorRegistry collectorRegistry = CollectorRegistry.defaultRegistry;
        //
        new JobMetricExports(this).register(collectorRegistry);
        new ThreadPoolStatusExports(this).register(collectorRegistry);
    }

    public Node getNode() {
        return node;
    }
}
