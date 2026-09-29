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

package org.apache.seatunnel.engine.server.master.cleanup;

import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.serializable.ResourceDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JobCleanupRecord implements IdentifiedDataSerializable {

    private long ownerInitializationTimestamp;
    private JobStatus finalStatus;
    private Set<Object> stateKeys = new LinkedHashSet<>();
    private Set<Object> timestampKeys = new LinkedHashSet<>();
    private long createTimeMillis;

    @Override
    public int getFactoryId() {
        return ResourceDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ResourceDataSerializerHook.JOB_CLEANUP_RECORD_TYPE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(ownerInitializationTimestamp);
        out.writeString(finalStatus == null ? null : finalStatus.name());
        writeSet(out, stateKeys);
        writeSet(out, timestampKeys);
        out.writeLong(createTimeMillis);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        ownerInitializationTimestamp = in.readLong();
        String statusName = in.readString();
        finalStatus = statusName == null ? null : JobStatus.valueOf(statusName);
        stateKeys = readSet(in);
        timestampKeys = readSet(in);
        createTimeMillis = in.readLong();
    }

    private void writeSet(ObjectDataOutput out, Set<Object> values) throws IOException {
        if (values == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(values.size());
        for (Object value : values) {
            out.writeObject(value);
        }
    }

    private Set<Object> readSet(ObjectDataInput in) throws IOException {
        int size = in.readInt();
        if (size < 0) {
            return null;
        }
        Set<Object> values = new LinkedHashSet<>(size);
        for (int i = 0; i < size; i++) {
            values.add(in.readObject());
        }
        return values;
    }
}
