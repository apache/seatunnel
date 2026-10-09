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

package org.apache.seatunnel.engine.server.task.source;

import org.apache.seatunnel.engine.server.serializable.TaskDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/** Version-stable admission response returned without executing connector code. */
public final class SourceCommandAdmissionAck implements IdentifiedDataSerializable {
    private SourceCommandAdmissionStatus status;
    private String commandId;
    private long expectedSequence;
    private long retryAfterMillis;
    private String detail;

    public SourceCommandAdmissionAck() {}

    SourceCommandAdmissionAck(
            SourceCommandAdmissionStatus status,
            String commandId,
            long expectedSequence,
            long retryAfterMillis,
            String detail) {
        if (status == null) {
            throw new IllegalArgumentException("Managed Source admission status must not be null");
        }
        if (expectedSequence < -1L || retryAfterMillis < 0L) {
            throw new IllegalArgumentException(
                    "Managed Source admission sequence and retry delay are invalid");
        }
        this.status = status;
        this.commandId = requireBoundedText(commandId, "commandId", 256);
        this.expectedSequence = expectedSequence;
        this.retryAfterMillis = retryAfterMillis;
        this.detail = requireBoundedText(detail, "detail", 4096);
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeInt(status.getCode());
        out.writeString(commandId);
        out.writeLong(expectedSequence);
        out.writeLong(retryAfterMillis);
        out.writeString(detail);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        try {
            status = SourceCommandAdmissionStatus.fromCode(in.readInt());
        } catch (IllegalArgumentException e) {
            throw new IOException("Unknown managed Source admission status", e);
        }
        try {
            commandId = requireBoundedText(in.readString(), "commandId", 256);
            expectedSequence = in.readLong();
            retryAfterMillis = in.readLong();
            detail = requireBoundedText(in.readString(), "detail", 4096);
        } catch (IllegalArgumentException e) {
            throw new IOException("Invalid managed Source admission response", e);
        }
        if (expectedSequence < -1L || retryAfterMillis < 0L) {
            throw new IOException("Managed Source admission sequence and retry delay are invalid");
        }
    }

    @Override
    public int getFactoryId() {
        return TaskDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return TaskDataSerializerHook.MANAGED_SOURCE_ADMISSION_ACK;
    }

    public static SourceCommandAdmissionAck of(
            SourceCommandAdmissionStatus status,
            SourceCommandEnvelope command,
            long expectedSequence,
            long retryAfterMillis,
            String detail) {
        return new SourceCommandAdmissionAck(
                status,
                command == null ? "" : command.getCommandId(),
                expectedSequence,
                retryAfterMillis,
                detail == null ? "" : detail);
    }

    public SourceCommandAdmissionStatus getStatus() {
        return status;
    }

    public String getCommandId() {
        return commandId;
    }

    public long getExpectedSequence() {
        return expectedSequence;
    }

    public long getRetryAfterMillis() {
        return retryAfterMillis;
    }

    public String getDetail() {
        return detail;
    }

    public boolean acceptedOrDuplicate() {
        return status == SourceCommandAdmissionStatus.ACCEPTED
                || status == SourceCommandAdmissionStatus.DUPLICATE;
    }

    private static String requireBoundedText(String value, String field, int maxLength) {
        if (value == null || value.length() > maxLength) {
            throw new IllegalArgumentException(
                    "Managed Source admission " + field + " exceeds wire limit");
        }
        return value;
    }
}
