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

import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.serializable.JobDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import lombok.Getter;
import lombok.Setter;

import java.io.IOException;
import java.util.Objects;

@Getter
@Setter
public class LogicalVertex implements IdentifiedDataSerializable {

    private Long vertexId;
    private Action action;

    /**
     * Number of subtasks to split this task into at runtime.
     */
    private int parallelism;

    public LogicalVertex() {
    }

    public LogicalVertex(Long vertexId, Action action, int parallelism) {
        this.vertexId = vertexId;
        this.action = action;
        this.parallelism = parallelism;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        LogicalVertex that = (LogicalVertex) o;
        return Objects.equals(vertexId, that.vertexId)
            && Objects.equals(action, that.action);
    }

    @Override
    public String toString() {
        return "LogicalVertex{" +
            "jobVertexId=" + vertexId +
            ", action=" + action +
            ", parallelism=" + parallelism +
            '}';
    }

    @Override
    public int hashCode() {
        return Objects.hash(vertexId, action);
    }

    @Override
    public int getFactoryId() {
        return JobDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JobDataSerializerHook.LOGICAL_VERTEX;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(vertexId);
        out.writeObject(action);
        out.writeInt(parallelism);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        vertexId = in.readLong();
        action = in.readObject();
        parallelism = in.readInt();
    }
}
