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

package org.apache.seatunnel.engine.core.job;

import org.apache.seatunnel.engine.core.serializable.JobDataSerializerHook;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import lombok.Data;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Replicated job metadata stored as a standalone Hazelcast {@code Data} value.
 *
 * <p>Legacy metadata has no trailing enqueue sequence. Detecting that field relies on this object
 * occupying the remainder of its serialization buffer: bytes following the legacy prefix must
 * belong to this object. Do not embed its raw {@link #writeData(ObjectDataOutput)} output before
 * sibling fields in a shared stream; serialize it as a separate {@code Data} value instead.
 */
@Data
public class JobInfo implements IdentifiedDataSerializable {
    private Long initializationTimestamp;

    private com.hazelcast.internal.serialization.Data jobImmutableInformation;

    /** Zero identifies legacy metadata or a submission that has not entered the queue yet. */
    private long enqueueSequence;

    public JobInfo() {}

    public JobInfo(
            Long initializationTimestamp,
            com.hazelcast.internal.serialization.Data jobImmutableInformation) {
        this.initializationTimestamp = initializationTimestamp;
        this.jobImmutableInformation = jobImmutableInformation;
    }

    @Override
    public int getFactoryId() {
        return JobDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return JobDataSerializerHook.JOB_INFO;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(initializationTimestamp);
        IOUtil.writeData(out, jobImmutableInformation);
        out.writeLong(enqueueSequence);
    }

    /**
     * Reads the legacy field prefix and, for Hazelcast buffer inputs, the optional enqueue
     * sequence.
     *
     * <p>The remaining-byte check depends on Hazelcast's buffer implementations exposing {@code
     * available()}, which is not part of the {@code BufferObjectDataInput} interface. Keep the safe
     * and unsafe input compatibility tests when changing Hazelcast versions. Other input types
     * retain the legacy fallback (sequence zero) and do not consume an extension; they must not be
     * used to restore metadata that requires the enqueue-order guarantee.
     */
    @Override
    public void readData(ObjectDataInput in) throws IOException {
        initializationTimestamp = in.readLong();
        jobImmutableInformation = IOUtil.readData(in);
        enqueueSequence = 0;
        // JobInfo is a top-level IMap value. Keep the legacy prefix readable and accept values
        // written before queue ordering was recorded, including Hazelcast's unsafe input.
        if (in instanceof BufferObjectDataInput) {
            try {
                Method available = in.getClass().getMethod("available");
                available.setAccessible(true);
                int remaining = (Integer) available.invoke(in);
                if (remaining > 0 && remaining < Long.BYTES) {
                    throw new IOException("Truncated JobInfo enqueue sequence");
                }
                if (remaining > 0) {
                    enqueueSequence = in.readLong();
                }
            } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
                throw new IOException("Failed to inspect remaining JobInfo bytes", e);
            }
        }
    }
}
