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

import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

/** Embedded Zeta environment with observability and a bounded async boundary enabled. */
@State(Scope.Thread)
public class SeaTunnelObservabilityEnvironmentContext extends SeaTunnelEnvironmentContext {

    private static final String JOB_CONFIG_TEMPLATE =
            BenchmarkTemplates.load("/benchmark/source-transform-sink-observability.conf.template");

    @Override
    protected String jobConfigTemplate(BenchmarkPipeline pipeline) {
        return JOB_CONFIG_TEMPLATE;
    }
}
