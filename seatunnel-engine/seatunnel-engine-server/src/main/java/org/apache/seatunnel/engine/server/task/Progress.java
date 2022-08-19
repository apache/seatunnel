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

package org.apache.seatunnel.engine.server.task;

import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;
import java.io.Serializable;

public class Progress implements IdentifiedDataSerializable, Serializable {

    private boolean madeProgress;
    private boolean isDone;

    public Progress() {
        isDone = true;
        madeProgress = false;
    }

    public void start() {
        isDone = false;
        madeProgress = false;
    }

    public void makeProgress() {
        isDone = false;
        madeProgress = true;
    }

    public void done() {
        isDone = true;
    }

    public ProgressState toState() {
        return ProgressState.valueOf(madeProgress, isDone);
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.PROGRESS_TYPE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeBoolean(isDone);
        out.writeBoolean(madeProgress);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        isDone = in.readBoolean();
        madeProgress = in.readBoolean();
    }
}
