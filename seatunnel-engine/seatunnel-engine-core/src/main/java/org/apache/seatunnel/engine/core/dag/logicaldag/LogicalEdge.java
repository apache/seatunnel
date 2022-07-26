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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.seatunnel.engine.core.serializable.JobDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import lombok.Data;
import lombok.NonNull;

import java.io.IOException;
import java.util.Map;

@Data
public class LogicalEdge implements IdentifiedDataSerializable {
    private LogicalVertex leftVertex;
    private LogicalVertex rightVertex;

    private Integer leftVertexId;

    private Integer rightVertexId;

    public LogicalEdge(){}

    public LogicalEdge(LogicalVertex leftVertex, LogicalVertex rightVertex) {
        this.leftVertex = leftVertex;
        this.rightVertex = rightVertex;
        this.leftVertexId = leftVertex.getVertexId();
        this.rightVertexId = rightVertex.getVertexId();
    }

    public void recoveryFromVertexMap(@NonNull Map<Integer, LogicalVertex> vertexMap) {
        leftVertex = vertexMap.get(leftVertexId);
        rightVertex = vertexMap.get(rightVertexId);

        checkNotNull(leftVertex);
        checkNotNull(rightVertex);
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
        out.writeInt(leftVertexId);
        out.writeInt(rightVertexId);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        leftVertexId = in.readInt();
        rightVertexId = in.readInt();
    }
}
