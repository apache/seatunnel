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

package org.apache.seatunnel.engine.server.task.operation.source;

import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;
import org.apache.seatunnel.engine.server.task.operation.TaskOperation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

public abstract class SourceEventOperation extends TaskOperation {
    protected TaskLocation currentTaskLocation;

    protected byte[] sourceEvent;

    public SourceEventOperation() {
    }

    public SourceEventOperation(TaskLocation targetTaskLocation,
                                TaskLocation currentTaskLocation,
                                SourceEvent event) {
        super(targetTaskLocation);
        this.currentTaskLocation = currentTaskLocation;
        this.sourceEvent = SerializationUtils.serialize(event);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(currentTaskLocation);
        out.writeObject(sourceEvent);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        currentTaskLocation = in.readObject();
        sourceEvent = in.readObject();
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }
}
