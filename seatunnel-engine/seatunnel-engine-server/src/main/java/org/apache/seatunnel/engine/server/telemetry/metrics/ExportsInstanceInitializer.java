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

import org.apache.seatunnel.engine.server.telemetry.metrics.exports.ClusterMetricExports;
import org.apache.seatunnel.engine.server.telemetry.metrics.exports.JobMetricExports;
import org.apache.seatunnel.engine.server.telemetry.metrics.exports.JobThreadPoolStatusExports;
import org.apache.seatunnel.engine.server.telemetry.metrics.exports.NodeMetricExports;

import com.hazelcast.instance.impl.Node;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.hotspot.DefaultExports;

public final class ExportsInstanceInitializer {

    private static boolean initialized = false;

    private ExportsInstanceInitializer() {}

    public static synchronized void init(Node node) {
        if (!initialized) {
            // initialize jvm collector
            DefaultExports.initialize();

            // register collectors
            CollectorRegistry collectorRegistry = CollectorRegistry.defaultRegistry;
            // Job info detail
            new JobMetricExports(node).register(collectorRegistry);
            // Thread pool status
            new JobThreadPoolStatusExports(node).register(collectorRegistry);
            // Node metrics
            new NodeMetricExports(node).register(collectorRegistry);
            // Cluster metrics
            new ClusterMetricExports(node).register(collectorRegistry);
            initialized = true;
        }
    }
}
