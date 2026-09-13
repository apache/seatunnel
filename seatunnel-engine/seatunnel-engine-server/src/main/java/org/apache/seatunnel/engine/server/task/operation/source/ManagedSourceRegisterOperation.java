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

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;
import org.apache.seatunnel.engine.server.task.SourceSplitEnumeratorTask;
import org.apache.seatunnel.engine.server.task.operation.TaskOperation;
import org.apache.seatunnel.engine.server.task.source.ManagedSourceRegistration;
import org.apache.seatunnel.engine.server.task.source.SourceCommandAdmissionStatus;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/** Admits an attempt-aware Reader registration without invoking the connector enumerator. */
public class ManagedSourceRegisterOperation extends TaskOperation {
    private TaskLocation readerLocation;
    /** Engine deployment identity used before the random attempt ID is admitted. */
    private long readerExecutionId;

    private String readerAttemptId;
    private int runtimeProtocolVersion;
    private String capabilityDigest;
    private long firstReaderCommandSequence;
    private long restoredAppliedWatermark;
    /** Durable no-more-splits generation restored by the registering Reader. */
    private long restoredNoMoreSplitsGeneration;

    private int responseCode = SourceCommandAdmissionStatus.STALE_TARGET.getCode();

    public ManagedSourceRegisterOperation() {}

    public ManagedSourceRegisterOperation(
            TaskLocation enumeratorLocation,
            TaskLocation readerLocation,
            long readerExecutionId,
            String readerAttemptId,
            int runtimeProtocolVersion,
            String capabilityDigest,
            long firstReaderCommandSequence,
            long restoredAppliedWatermark,
            long restoredNoMoreSplitsGeneration) {
        super(enumeratorLocation);
        this.readerLocation = readerLocation;
        this.readerExecutionId = readerExecutionId;
        this.readerAttemptId = readerAttemptId;
        this.runtimeProtocolVersion = runtimeProtocolVersion;
        this.capabilityDigest = capabilityDigest;
        this.firstReaderCommandSequence = firstReaderCommandSequence;
        this.restoredAppliedWatermark = restoredAppliedWatermark;
        this.restoredNoMoreSplitsGeneration = restoredNoMoreSplitsGeneration;
    }

    @Override
    public void runInternal() {
        try {
            SeaTunnelServer server = getService();
            SourceSplitEnumeratorTask<?> task =
                    server.getTaskExecutionService().getTask(taskLocation);
            responseCode =
                    task.admitManagedReaderRegistration(
                                    new ManagedSourceRegistration(
                                            readerLocation,
                                            getCallerAddress(),
                                            readerExecutionId,
                                            readerAttemptId,
                                            runtimeProtocolVersion,
                                            capabilityDigest,
                                            firstReaderCommandSequence,
                                            restoredAppliedWatermark,
                                            restoredNoMoreSplitsGeneration))
                            .getCode();
        } catch (Exception e) {
            responseCode = SourceCommandAdmissionStatus.STALE_TARGET.getCode();
        }
    }

    @Override
    public Object getResponse() {
        return responseCode;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(readerLocation);
        out.writeLong(readerExecutionId);
        out.writeString(readerAttemptId);
        out.writeInt(runtimeProtocolVersion);
        out.writeString(capabilityDigest);
        out.writeLong(firstReaderCommandSequence);
        out.writeLong(restoredAppliedWatermark);
        out.writeLong(restoredNoMoreSplitsGeneration);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        readerLocation = in.readObject();
        readerExecutionId = in.readLong();
        readerAttemptId = in.readString();
        runtimeProtocolVersion = in.readInt();
        capabilityDigest = in.readString();
        firstReaderCommandSequence = in.readLong();
        restoredAppliedWatermark = in.readLong();
        restoredNoMoreSplitsGeneration = in.readLong();
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.MANAGED_SOURCE_REGISTER_OPERATION;
    }

    @Override
    public String getServiceName() {
        return SeaTunnelServer.SERVICE_NAME;
    }
}
