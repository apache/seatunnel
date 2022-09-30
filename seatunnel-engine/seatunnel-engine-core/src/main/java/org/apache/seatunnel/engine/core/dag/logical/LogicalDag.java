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

package org.apache.seatunnel.engine.core.dag.logical;

import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.serializable.JobDataSerializerHook;

import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import lombok.Getter;
import lombok.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/**
 * A LogicalDag describe the logical plan run by SeaTunnel Engine
 * {@link LogicalVertex} defines an operator, and {@link LogicalEdge} defines the
 * relationship between the two operators.
 * <p>
 * {@link LogicalVertex} not a final executable object. It will be optimized when
 * generate PhysicalDag in JobMaster.
 * <p>
 * There are three basic kinds of vertices:
 * <ol><li>
 *     <em>SeaTunnelSource</em> with just outbound edges;
 * </li><li>
 *     <em>SeaTunnelTransform</em> with both inbound and outbound edges;
 * </li><li>
 *     <em>SeaTunnelSink</em> with just inbound edges.
 * </li></ol>
 * Data travels from sources to sinks and is transformed and reshaped
 * as it passes through the processors.
 */
public class LogicalDag implements IdentifiedDataSerializable {

    private static final Logger LOG = LoggerFactory.getLogger(LogicalDag.class);
    @Getter
    private JobConfig jobConfig;
    private final Set<LogicalEdge> edges = new LinkedHashSet<>();
    private final Map<Long, LogicalVertex> logicalVertexMap = new LinkedHashMap<>();
    private IdGenerator idGenerator;

    public LogicalDag() {
    }

    public LogicalDag(@NonNull JobConfig jobConfig,
                      @NonNull IdGenerator idGenerator) {
        this.jobConfig = jobConfig;
        this.idGenerator = idGenerator;
    }

    public void addLogicalVertex(LogicalVertex logicalVertex) {
        logicalVertexMap.put(logicalVertex.getVertexId(), logicalVertex);
    }

    public void addEdge(LogicalEdge logicalEdge) {
        edges.add(logicalEdge);
    }

    public Set<LogicalEdge> getEdges() {
        return this.edges;
    }

    public Map<Long, LogicalVertex> getLogicalVertexMap() {
        return logicalVertexMap;
    }

    @NonNull
    public JsonObject getLogicalDagAsJson() {
        JsonObject logicalDag = new JsonObject();
        JsonArray vertices = new JsonArray();

        logicalVertexMap.values().stream().forEach(v -> {
            JsonObject vertex = new JsonObject();
            vertex.add("id", v.getVertexId());
            vertex.add("name", v.getAction().getName() + "(id=" + v.getVertexId() + ")");
            vertex.add("parallelism", v.getParallelism());
            vertices.add(vertex);
        });
        logicalDag.add("vertices", vertices);

        JsonArray edges = new JsonArray();
        this.edges.stream().forEach(e -> {
            JsonObject edge = new JsonObject();
            edge.add("inputVertex", e.getInputVertex().getAction().getName());
            edge.add("targetVertex", e.getTargetVertex().getAction().getName());
            edges.add(edge);
        });

        logicalDag.add("edges", edges);
        return logicalDag;
    }

    @Override
    public int getFactoryId() {
        return JobDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JobDataSerializerHook.LOGICAL_DAG;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(logicalVertexMap.size());

        for (Map.Entry<Long, LogicalVertex> entry : logicalVertexMap.entrySet()) {
            out.writeLong(entry.getKey());
            out.writeObject(entry.getValue());
        }

        out.writeInt(edges.size());

        for (LogicalEdge edge : edges) {
            out.writeObject(edge);
        }

        out.writeObject(jobConfig);
        out.writeObject(idGenerator);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int vertexCount = in.readInt();

        for (int i = 0; i < vertexCount; i++) {
            Long key = in.readLong();
            LogicalVertex value = in.readObject();
            logicalVertexMap.put(key, value);
        }

        int edgeCount = in.readInt();

        for (int i = 0; i < edgeCount; i++) {
            LogicalEdge edge = in.readObject();
            edge.recoveryFromVertexMap(logicalVertexMap);
            edges.add(edge);
        }

        jobConfig = in.readObject();
        idGenerator = in.readObject();
    }
}
