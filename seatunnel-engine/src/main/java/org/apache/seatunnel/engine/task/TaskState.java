/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.task;

import org.apache.seatunnel.engine.api.serialization.Serializer;

import java.io.IOException;

public class TaskState implements Serializer<TaskState> {
    int checkpointId;
    byte[] sourceState;
    byte[] sinkState;

    public TaskState(int checkpointId, byte[] sourceState, byte[] sinkState) {
        this.checkpointId = checkpointId;
        this.sourceState = sourceState;
        this.sinkState = sinkState;
    }

    public int getCheckpointId() {
        return checkpointId;
    }

    public void setCheckpointId(int checkpointId) {
        this.checkpointId = checkpointId;
    }

    public byte[] getSourceState() {
        return sourceState;
    }

    public void setSourceState(byte[] sourceState) {
        this.sourceState = sourceState;
    }

    public byte[] getSinkState() {
        return sinkState;
    }

    public void setSinkState(byte[] sinkState) {
        this.sinkState = sinkState;
    }

    @Override
    public byte[] serialize(TaskState obj) throws IOException {
        return new byte[0];
    }

    @Override
    public TaskState deserialize(byte[] serialized) throws IOException {
        return null;
    }
}
