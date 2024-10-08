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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.SerializationFeature;

import org.apache.seatunnel.api.table.catalog.TablePath;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
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

        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        JsonObject jsonObject = new JsonObject();
        jsonObject.add("jobId", jobId.toString());
        jsonObject.add("pipelineEdges", pipelineEdgesJsonObject);
        JsonObject vertexInfoMapString = new JsonObject();
        for (Map.Entry<Long, VertexInfo> entry : vertexInfoMap.entrySet()) {
            JsonArray jsonArray = new JsonArray();
            for (TablePath tablePath : entry.getValue().getTablePaths()) {
                jsonArray.add(tablePath.toString());
            }
            vertexInfoMapString.add(entry.getKey().toString(), jsonArray);
        }
        jsonObject.add("vertexInfoMap", vertexInfoMapString);
        return jsonObject;
    }
}
