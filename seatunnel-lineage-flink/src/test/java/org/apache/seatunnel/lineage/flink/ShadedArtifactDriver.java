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

package org.apache.seatunnel.lineage.flink;

import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageDataset;
import org.apache.seatunnel.lineage.LineageEvent;
import org.apache.seatunnel.lineage.LineageEventType;
import org.apache.seatunnel.lineage.LineageOutputStatistics;
import org.apache.seatunnel.lineage.LineageRuntime;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Emits one event, run by {@link ShadedArtifactIT} in a JVM whose only class path entries are the
 * shaded artifact and slf4j-api. It stands in for the JobManager, which has exactly that.
 */
public final class ShadedArtifactDriver {

    private ShadedArtifactDriver() {}

    public static void main(String[] args) throws Exception {
        Map<String, Object> options = new LinkedHashMap<>();
        options.put(LineageConfig.ENABLED, true);
        options.put(LineageConfig.URL, args[0]);
        Map<String, Object> properties = new LinkedHashMap<>();
        properties.put("engine", "flink");
        properties.put(LineageJobStatusHook.FLINK_JOB_ID_PROPERTY, "shaded-artifact-it");
        options.put(LineageConfig.RUN_PROPERTIES, properties);

        LineageConfig config =
                LineageConfig.resolve(
                        options,
                        Collections.<String, Object>emptyMap(),
                        Collections.<String, Object>emptyMap());

        LineageEvent event =
                LineageEvent.builder()
                        .runId(UUID.randomUUID())
                        .eventTime(ZonedDateTime.now(ZoneOffset.UTC))
                        .eventType(LineageEventType.COMPLETE)
                        .jobNamespace("seatunnel")
                        .jobName("shaded-artifact-it")
                        .producer("https://seatunnel.apache.org/shaded-artifact-it")
                        .runFacet(config.runFacet())
                        .runProperties(properties)
                        .inputs(
                                Collections.singletonList(
                                        LineageDataset.of("mysql://source:9030", "sales.orders")))
                        .outputs(
                                Collections.singletonList(
                                        LineageDataset.of("mysql://sink:9030", "dw.orders")))
                        .build()
                        .withOutputStatistics(new LineageOutputStatistics(7L, 128L, "attempted"));

        LineageRuntime.emit(config, event);
    }
}
