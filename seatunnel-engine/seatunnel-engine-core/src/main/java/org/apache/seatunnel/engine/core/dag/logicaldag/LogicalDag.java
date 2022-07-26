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

package org.apache.seatunnel.engine.core.dag.logicaldag;

import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.core.serializable.JobDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
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
    private JobConfig jobConfig;
    private Set<LogicalEdge> edges = new LinkedHashSet<>();
    private Map<Integer, LogicalVertex> logicalVertexMap = new HashMap<>();

    @Override
    public int getFactoryId() {
        return JobDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JobDataSerializerHook.LOGICAL_DAG;
    }

    public void addLogicalVertex(LogicalVertex logicalVertex) {
        logicalVertexMap.put(logicalVertex.getVertexId(), logicalVertex);
    }

    public void addEdge(LogicalEdge logicalEdge) {
        edges.add(logicalEdge);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(logicalVertexMap.size());

        for (Map.Entry<Integer, LogicalVertex> entry : logicalVertexMap.entrySet()) {
            out.writeInt(entry.getKey());
            out.writeObject(entry.getValue());
        }

        out.writeInt(edges.size());

        for (LogicalEdge edge : edges) {
            out.writeObject(edge);
        }

        out.writeObject(jobConfig);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        int vertexCount = in.readInt();

        for (int i = 0; i < vertexCount; i++) {
            Integer key = in.readInt();
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
    }
}
