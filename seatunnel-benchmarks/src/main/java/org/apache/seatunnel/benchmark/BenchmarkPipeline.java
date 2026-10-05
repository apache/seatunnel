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

/** Full-pipeline scenarios supported by the embedded Zeta benchmark contexts. */
public enum BenchmarkPipeline {
    SOURCE_SINK("source-sink", false),
    SOURCE_TRANSFORM_SINK("source-transform-sink", true),
    SOURCE_TRANSFORM_SINK_OBSERVABILITY("source-transform-sink-observability", true),
    SOURCE_TRANSFORM_SINK_TRACE("source-transform-sink-trace", true),
    SOURCE_TRANSFORM_SINK_OBSERVABILITY_TRACE("source-transform-sink-observability-trace", true);

    private final String id;
    private final boolean transformEnabled;

    BenchmarkPipeline(String id, boolean transformEnabled) {
        this.id = id;
        this.transformEnabled = transformEnabled;
    }

    public String getId() {
        return id;
    }

    public boolean isTransformEnabled() {
        return transformEnabled;
    }
}
