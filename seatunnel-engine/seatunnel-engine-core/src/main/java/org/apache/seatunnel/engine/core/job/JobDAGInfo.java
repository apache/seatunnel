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

@AllArgsConstructor
@NoArgsConstructor
@Data
public class JobDAGInfo implements Serializable {
    Long jobId;
    Map<String, Object> envOptions;
    Map<Integer, List<Edge>> pipelineEdges;
    Map<Long, VertexInfo> vertexInfoMap;

    public JsonObject toJsonObject() {
        JsonObject pipelineEdgesJsonObject = new JsonObject();

        for (Map.Entry<Integer, List<Edge>> entry : pipelineEdges.entrySet()) {
            JsonArray jsonArray = new JsonArray();
            for (Edge edge : entry.getValue()) {
                JsonObject edgeJsonObject = new JsonObject();
                edgeJsonObject.add("inputVertexId", edge.getInputVertexId().toString());
                edgeJsonObject.add("targetVertexId", edge.getTargetVertexId().toString());
                jsonArray.add(edgeJsonObject);
            }
            pipelineEdgesJsonObject.add(entry.getKey().toString(), jsonArray);
        }

        JsonObject jsonObject = new JsonObject();
        jsonObject.add("jobId", jobId.toString());
        jsonObject.add("pipelineEdges", pipelineEdgesJsonObject);
        jsonObject.add("envOptions", JsonUtil.toJsonObject(envOptions));

        JsonArray vertexInfoMapString = new JsonArray();
        for (Map.Entry<Long, VertexInfo> entry : vertexInfoMap.entrySet()) {
            JsonObject vertexInfoJsonObj = new JsonObject();
            VertexInfo vertexInfo = entry.getValue();
            vertexInfoJsonObj.add("vertexId", vertexInfo.getVertexId());
            vertexInfoJsonObj.add("type", vertexInfo.getType().getType());
            vertexInfoJsonObj.add("vertexName", vertexInfo.getConnectorType());
            JsonArray tablePaths = new JsonArray();
            for (TablePath tablePath : vertexInfo.getTablePaths()) {
                tablePaths.add(tablePath.toString());
            }
            vertexInfoJsonObj.add("tablePaths", tablePaths);
            vertexInfoMapString.add(vertexInfoJsonObj);
        }
        jsonObject.add("vertexInfoMap", vertexInfoMapString);
        return jsonObject;
    }
}
