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
import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;
import org.apache.seatunnel.engine.server.task.SourceSeaTunnelTask;
import org.apache.seatunnel.engine.server.task.operation.TaskOperation;
import org.apache.seatunnel.engine.server.task.source.SourceCommandAdmissionAck;
import org.apache.seatunnel.engine.server.task.source.SourceCommandAdmissionStatus;
import org.apache.seatunnel.engine.server.task.source.SourceCommandDurability;
import org.apache.seatunnel.engine.server.task.source.SourceCommandEnvelope;
import org.apache.seatunnel.engine.server.task.source.SourceCommandKind;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Hazelcast adapter that performs bounded admission only.
 *
 * <p>This operation never deserializes connector payloads, invokes connector code, sleeps, retries,
 * or waits for command application.
 */
public class ManagedSourceCommandOperation extends TaskOperation {
    private SourceCommandEnvelope command;
    private SourceCommandAdmissionAck response;

    public ManagedSourceCommandOperation() {}

    public ManagedSourceCommandOperation(
            org.apache.seatunnel.engine.server.execution.TaskLocation taskLocation,
            SourceCommandEnvelope command) {
        super(taskLocation);
        this.command = command;
    }

    @Override
    public void runInternal() {
        try {
            SeaTunnelServer server = getService();
            Object task = server.getTaskExecutionService().getTask(taskLocation);
            if (!(task instanceof SourceSeaTunnelTask)) {
                response =
                        SourceCommandAdmissionAck.of(
                                SourceCommandAdmissionStatus.STALE_TARGET,
                                command,
                                -1L,
                                0L,
                                "Target is not an active Source reader task");
                return;
            }
            response = ((SourceSeaTunnelTask<?, ?>) task).admitManagedSourceCommand(command);
        } catch (Exception e) {
            response =
                    SourceCommandAdmissionAck.of(
                            SourceCommandAdmissionStatus.STALE_TARGET,
                            command,
                            -1L,
                            0L,
                            "Target Source task is unavailable: " + e.getClass().getSimpleName());
        }
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.MANAGED_SOURCE_COMMAND_OPERATION;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(command.getProtocolVersion());
        out.writeLong(command.getJobId());
        out.writeLong(command.getSourceRuntimeId());
        out.writeString(command.getCoordinatorEpoch());
        out.writeString(command.getSenderAttemptId());
        out.writeString(command.getTargetAttemptId());
        out.writeLong(command.getSenderSequence());
        out.writeString(command.getCommandId());
        out.writeInt(command.getKind().getCode());
        out.writeInt(command.getDurability().getCode());
        out.writeInt(command.getPayloadVersion());
        out.writeInt(command.getCodecId());
        out.writeString(command.getAssignmentGroupId());
        out.writeInt(command.getChunkIndex());
        out.writeInt(command.getChunkCount());
        out.writeLong(command.getChecksum());
        byte[] payload = command.getPayload();
        out.writeInt(payload.length);
        out.write(payload);
        out.writeLong(command.getAdmittedNanos());
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        int protocolVersion = in.readInt();
        long jobId = in.readLong();
        long sourceRuntimeId = in.readLong();
        String coordinatorEpoch = in.readString();
        String senderAttemptId = in.readString();
        String targetAttemptId = in.readString();
        long senderSequence = in.readLong();
        String commandId = in.readString();
        SourceCommandKind kind;
        SourceCommandDurability durability;
        try {
            kind = SourceCommandKind.fromCode(in.readInt());
            durability = SourceCommandDurability.fromCode(in.readInt());
        } catch (IllegalArgumentException e) {
            throw new IOException("Unknown managed Source command header", e);
        }
        int payloadVersion = in.readInt();
        int codecId = in.readInt();
        String assignmentGroupId = in.readString();
        int chunkIndex = in.readInt();
        int chunkCount = in.readInt();
        long checksum = in.readLong();
        int payloadLength = in.readInt();
        if (payloadLength < 0 || payloadLength > SourceCommandEnvelope.MAX_WIRE_PAYLOAD_BYTES) {
            throw new IOException("Managed Source command payload exceeds wire hard limit");
        }
        byte[] payload = new byte[payloadLength];
        in.readFully(payload);
        try {
            command =
                    new SourceCommandEnvelope(
                            protocolVersion,
                            jobId,
                            sourceRuntimeId,
                            coordinatorEpoch,
                            senderAttemptId,
                            targetAttemptId,
                            senderSequence,
                            commandId,
                            kind,
                            durability,
                            payloadVersion,
                            codecId,
                            assignmentGroupId,
                            chunkIndex,
                            chunkCount,
                            checksum,
                            payload,
                            in.readLong());
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid managed Source command header", e);
        }
    }

    @Override
    public String getServiceName() {
        return SeaTunnelServer.SERVICE_NAME;
    }
}
