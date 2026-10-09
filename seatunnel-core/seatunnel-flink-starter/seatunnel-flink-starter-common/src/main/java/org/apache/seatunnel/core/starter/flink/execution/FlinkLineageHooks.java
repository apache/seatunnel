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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageEvent;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import java.util.List;

/**
 * The Flink-version-specific half of lineage registration, absent on this starter.
 *
 * <p>{@code JobStatusHook} is a 1.16+ API, so the hook can only be built and registered by a
 * starter that compiles against a Flink new enough to declare it. This module compiles against 1.15
 * and therefore answers that lineage is unsupported; {@code seatunnel-flink-20-starter} carries a
 * class of the same name that does the real work, and its copy wins in the shaded starter jar. That
 * is the same per-version override the starters already use for {@link SinkExecuteProcessor}.
 *
 * <p>Keeping the version-specific step behind this seam is what lets {@link FlinkLineageSupport}
 * and {@link FlinkExecution} stay version-agnostic without reflecting over the hook API.
 */
final class FlinkLineageHooks {

    private FlinkLineageHooks() {}

    /** Returns whether this starter can register a job status hook. */
    static boolean isSupported() {
        return false;
    }

    /**
     * Registers the JobManager-side status hook and the client-side statistics listener.
     *
     * @param streamGraph the graph the hook is attached to
     * @param environment the environment the result listener is registered on
     * @param config lineage configuration with the auth token already removed
     * @param events the start events whose runs the hook closes
     * @param clientReportsTerminalEvent whether the client reports the successful terminal event
     * @return whether the hook was registered
     */
    static boolean register(
            StreamGraph streamGraph,
            StreamExecutionEnvironment environment,
            LineageConfig config,
            List<LineageEvent> events,
            boolean clientReportsTerminalEvent) {
        return false;
    }
}
