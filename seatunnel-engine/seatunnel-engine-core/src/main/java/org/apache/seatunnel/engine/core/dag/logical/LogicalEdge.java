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

import org.apache.seatunnel.engine.core.serializable.JobDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import lombok.Data;

import java.io.IOException;

@Data
public class LogicalEdge implements IdentifiedDataSerializable {

    /** The input vertex connected to this edge. */
    private LogicalVertex inputVertex;

    /** The target vertex connected to this edge. */
    private LogicalVertex targetVertex;

    private Long inputVertexId;

    private Long targetVertexId;

    public LogicalEdge() {}

    public LogicalEdge(Long inputVertexId, Long targetVertexId) {
        this.inputVertexId = inputVertexId;
        this.targetVertexId = targetVertexId;
    }

    public LogicalEdge(LogicalVertex inputVertex, LogicalVertex targetVertex) {
        this.inputVertexId = inputVertex.getVertexId();
        this.targetVertexId = targetVertex.getVertexId();
    }

    @Override
    public int getFactoryId() {
        return JobDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JobDataSerializerHook.LOGICAL_EDGE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        // To prevent circular serialization, we only serialize the ID of vertices for edges
        out.writeLong(inputVertexId);
        out.writeLong(targetVertexId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        inputVertexId = in.readLong();
        targetVertexId = in.readLong();
    }
}
