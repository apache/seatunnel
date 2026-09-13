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

package org.apache.seatunnel.lineage.openlineage;

import org.apache.seatunnel.lineage.LineageDataset;
import org.apache.seatunnel.lineage.LineageEvent;
import org.apache.seatunnel.lineage.LineageOutputStatistics;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClientUtils;

import java.net.URI;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Converts the small SeaTunnel contract into an OpenLineage RunEvent. */
final class OpenLineageEventConverter {
    private OpenLineageEventConverter() {}

    static String toJson(LineageEvent event) {
        OpenLineage openLineage = new OpenLineage(URI.create(event.producer()));
        OpenLineage.RunFacetsBuilder runFacetsBuilder = openLineage.newRunFacetsBuilder();
        Map<String, Object> runProperties = new LinkedHashMap<>(event.runProperties());
        String statisticsSemantics = statisticsSemantics(event.outputs());
        if (statisticsSemantics != null) {
            runProperties.put("output_statistics_semantics", statisticsSemantics);
        }
        if (!runProperties.isEmpty()) {
            OpenLineage.DefaultRunFacet facet =
                    new OpenLineage.DefaultRunFacet(URI.create(event.producer()));
            facet.getAdditionalProperties().putAll(runProperties);
            runFacetsBuilder.put(event.runFacet(), facet);
        }

        OpenLineage.Run run =
                openLineage
                        .newRunBuilder()
                        .runId(event.runId())
                        .facets(runFacetsBuilder.build())
                        .build();
        OpenLineage.Job job =
                openLineage
                        .newJobBuilder()
                        .namespace(event.jobNamespace())
                        .name(event.jobName())
                        .build();

        List<OpenLineage.InputDataset> inputs = new ArrayList<>();
        for (LineageDataset input : event.inputs()) {
            inputs.add(
                    openLineage
                            .newInputDatasetBuilder()
                            .namespace(input.namespace())
                            .name(input.name())
                            .build());
        }

        List<OpenLineage.OutputDataset> outputs = new ArrayList<>();
        for (LineageDataset output : event.outputs()) {
            OpenLineage.OutputDatasetBuilder outputBuilder =
                    openLineage
                            .newOutputDatasetBuilder()
                            .namespace(output.namespace())
                            .name(output.name());
            LineageOutputStatistics statistics = output.outputStatistics();
            if (statistics != null && statistics.hasValue()) {
                OpenLineage.OutputStatisticsOutputDatasetFacetBuilder statisticsBuilder =
                        openLineage.newOutputStatisticsOutputDatasetFacetBuilder();
                if (statistics.rowCount() != null) {
                    statisticsBuilder.rowCount(statistics.rowCount());
                }
                if (statistics.size() != null) {
                    statisticsBuilder.size(statistics.size());
                }
                outputBuilder.outputFacets(
                        openLineage
                                .newOutputDatasetOutputFacetsBuilder()
                                .outputStatistics(statisticsBuilder.build())
                                .build());
            }
            outputs.add(outputBuilder.build());
        }

        OpenLineage.RunEvent.EventType eventType =
                OpenLineage.RunEvent.EventType.valueOf(event.eventType().name());
        OpenLineage.RunEvent runEvent =
                openLineage
                        .newRunEventBuilder()
                        .eventTime(event.eventTime())
                        .eventType(eventType)
                        .run(run)
                        .job(job)
                        .inputs(inputs)
                        .outputs(outputs)
                        .build();
        return OpenLineageClientUtils.toJson(runEvent);
    }

    private static String statisticsSemantics(List<LineageDataset> outputs) {
        String semantics = null;
        for (LineageDataset output : outputs) {
            LineageOutputStatistics statistics = output.outputStatistics();
            if (statistics == null || statistics.semantics() == null) {
                continue;
            }
            if (semantics == null) {
                semantics = statistics.semantics();
            } else if (!semantics.equals(statistics.semantics())) {
                return "mixed";
            }
        }
        return semantics;
    }
}
