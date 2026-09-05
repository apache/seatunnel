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

package org.apache.seatunnel.benchmark;

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

/** Embedded Zeta environment with the StainTrace engine and local trace sink enabled. */
@State(Scope.Thread)
public class SeaTunnelTraceEnvironmentContext extends SeaTunnelEnvironmentContext {

    private static final int TRACE_SAMPLE_INTERVAL = 10_000;
    private static final int TRACE_FILE_FLUSH_INTERVAL_SECONDS = 1;
    private static final String JOB_CONFIG_TEMPLATE =
            BenchmarkTemplates.load("/benchmark/source-transform-sink-trace.conf.template");
    private static final String ENGINE_CONFIG_TEMPLATE =
            BenchmarkTemplates.load("/benchmark/engine-trace.yaml.template");

    @Override
    protected SeaTunnelConfig createSeaTunnelConfig(String name) {
        SeaTunnelConfig config = super.createSeaTunnelConfig(name);
        EngineConfig engineConfig = config.getEngineConfig();
        engineConfig.setStainTraceEnabled(true);
        engineConfig.setStainTraceSampleRate(TRACE_SAMPLE_INTERVAL);
        engineConfig.setStainTraceFileBasePath(getMiniClusterHome().toString());
        engineConfig.setStainTraceFileFlushIntervalSeconds(TRACE_FILE_FLUSH_INTERVAL_SECONDS);
        return config;
    }

    @Override
    protected String jobConfigTemplate(BenchmarkPipeline pipeline) {
        return JOB_CONFIG_TEMPLATE;
    }

    @Override
    protected String embeddedEngineConfiguration() {
        return BenchmarkTemplates.render(
                ENGINE_CONFIG_TEMPLATE,
                "slot_count",
                SLOT_COUNT,
                "trace_base_path",
                getMiniClusterHome());
    }
}
