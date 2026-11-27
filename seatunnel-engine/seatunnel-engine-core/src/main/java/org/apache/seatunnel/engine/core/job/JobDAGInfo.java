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

package org.apache.seatunnel.engine.core.job;

import org.apache.seatunnel.api.table.catalog.TablePath;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.util.JsonUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Set;

@AllArgsConstructor
@NoArgsConstructor
@Data
public class JobDAGInfo implements Serializable {
    private static final long serialVersionUID = 1L;

    Long jobId;
    Map<String, Object> envOptions;
    Map<Integer, List<Edge>> pipelineEdges;
    Map<Long, VertexInfo> vertexInfoMap;
    ExecutionAddress master;
    Set<ExecutionAddress> historyExecutionPlan;

    /**
     * Serializes this DAG description for the REST / Web UI job detail endpoints.
     *
     * <p>Every field may be {@code null}: the no-arg constructor leaves all of them unset, and the
     * info is read back from IMap storage where partially written entries are possible. A missing
     * part is rendered as an empty structure (or JSON null for scalars) instead of failing the
     * whole response with a {@link NullPointerException}.
     */
    public JsonObject toJsonObject() {
        JsonObject pipelineEdgesJsonObject = new JsonObject();

        if (pipelineEdges != null) {
            for (Map.Entry<Integer, List<Edge>> entry : pipelineEdges.entrySet()) {
                JsonArray jsonArray = new JsonArray();
                if (entry.getValue() != null) {
                    for (Edge edge : entry.getValue()) {
                        if (edge != null) {
                            JsonObject edgeJsonObject = new JsonObject();
                            edgeJsonObject.add(
                                    "inputVertexId", toStringOrNull(edge.getInputVertexId()));
                            edgeJsonObject.add(
                                    "targetVertexId", toStringOrNull(edge.getTargetVertexId()));
                            jsonArray.add(edgeJsonObject);
                        }
                    }
                }
                pipelineEdgesJsonObject.add(
                        entry.getKey() != null ? entry.getKey().toString() : "", jsonArray);
            }
        }

        JsonObject jsonObject = new JsonObject();
        jsonObject.add("jobId", toStringOrNull(jobId));
        jsonObject.add("pipelineEdges", pipelineEdgesJsonObject);
        jsonObject.add(
                "envOptions",
                envOptions != null ? JsonUtil.toJsonObject(envOptions) : new JsonObject());

        JsonArray vertexInfoMapString = new JsonArray();
        if (vertexInfoMap != null) {
            for (Map.Entry<Long, VertexInfo> entry : vertexInfoMap.entrySet()) {
                VertexInfo vertexInfo = entry.getValue();
                if (vertexInfo == null) {
                    continue;
                }
                JsonObject vertexInfoJsonObj = new JsonObject();
                vertexInfoJsonObj.add("vertexId", vertexInfo.getVertexId());
                vertexInfoJsonObj.add(
                        "type",
                        vertexInfo.getType() != null ? vertexInfo.getType().getType() : null);
                vertexInfoJsonObj.add("vertexName", vertexInfo.getConnectorType());
                JsonArray tablePaths = new JsonArray();
                if (vertexInfo.getTablePaths() != null) {
                    for (TablePath tablePath : vertexInfo.getTablePaths()) {
                        tablePaths.add(toStringOrNull(tablePath));
                    }
                }
                vertexInfoJsonObj.add("tablePaths", tablePaths);
                vertexInfoMapString.add(vertexInfoJsonObj);
            }
        }
        jsonObject.add("vertexInfoMap", vertexInfoMapString);
        return jsonObject;
    }

    /** Renders a possibly-null value as its string form, or JSON null when absent. */
    private static String toStringOrNull(Object value) {
        return value == null ? null : value.toString();
    }
}
