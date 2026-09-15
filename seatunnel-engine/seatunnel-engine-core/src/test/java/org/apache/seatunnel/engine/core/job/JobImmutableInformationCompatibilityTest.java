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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteOrder;
import java.util.Collections;

class JobImmutableInformationCompatibilityTest {

    private final InternalSerializationService serializationService =
            (InternalSerializationService) new DefaultSerializationServiceBuilder().build();

    private final InternalSerializationService unsafeSerializationService =
            (InternalSerializationService)
                    new DefaultSerializationServiceBuilder()
                            .setAllowUnsafe(true)
                            .setUseNativeByteOrder(true)
                            .setByteOrder(ByteOrder.nativeOrder())
                            .build();

    @Test
    void shouldReadLegacyPayload() throws Exception {
        long jobId = 123L;
        long createTime = 456L;

        JobImmutableInformation jobImmutableInformation =
                deserializeCurrentPayload(
                        serializeLegacyPayload(jobId, "legacy-job", true, createTime));

        Assertions.assertEquals(jobId, jobImmutableInformation.getJobId());
        Assertions.assertEquals("legacy-job", jobImmutableInformation.getJobName());
        Assertions.assertTrue(jobImmutableInformation.isStartWithSavePoint());
        Assertions.assertEquals(createTime, jobImmutableInformation.getCreateTime());
        Assertions.assertEquals(RestoreMode.SAVEPOINT, jobImmutableInformation.getRestoreMode());
        Assertions.assertEquals(jobId, jobImmutableInformation.getRestoreSourceJobId());
    }

    @Test
    void shouldWriteRestoreTrailerAfterLegacyFields() throws Exception {
        JobImmutableInformation jobImmutableInformation = new JobImmutableInformation();
        setField(jobImmutableInformation, "jobId", 123L);
        setField(jobImmutableInformation, "jobName", "checkpoint-job");
        setField(jobImmutableInformation, "isStartWithSavePoint", false);
        setField(jobImmutableInformation, "restoreMode", RestoreMode.CHECKPOINT);
        setField(jobImmutableInformation, "restoreSourceJobId", 456L);
        setField(jobImmutableInformation, "createTime", 789L);
        setField(jobImmutableInformation, "pluginJarsUrls", Collections.emptyList());
        setField(jobImmutableInformation, "connectorJarIdentifiers", Collections.emptyList());

        BufferObjectDataOutput out = serializationService.createObjectDataOutput();
        jobImmutableInformation.writeData(out);

        BufferObjectDataInput in = serializationService.createObjectDataInput(out.toByteArray());
        Assertions.assertEquals(123L, in.readLong());
        Assertions.assertEquals("checkpoint-job", in.readString());
        Assertions.assertFalse(in.readBoolean());
        Assertions.assertEquals(789L, in.readLong());
        Assertions.assertEquals(0, in.readInt());
        Assertions.assertNull(in.readData());
        Assertions.assertNull(in.readObject());
        Assertions.assertEquals(Collections.emptyList(), in.readObject());
        Assertions.assertEquals(Collections.emptyList(), in.readObject());
        Assertions.assertTrue(hasRemainingBytes(in));
        Assertions.assertEquals(RestoreMode.CHECKPOINT.getCode(), in.readInt());
        Assertions.assertTrue(in.readBoolean());
        Assertions.assertEquals(456L, in.readLong());
    }

    @Test
    void checkpointRestore_shouldBeRestoreJobButNotSavepointRestore() throws Exception {
        JobImmutableInformation jobImmutableInformation = new JobImmutableInformation();
        setField(jobImmutableInformation, "restoreMode", RestoreMode.CHECKPOINT);
        setField(jobImmutableInformation, "restoreSourceJobId", 456L);

        Assertions.assertTrue(jobImmutableInformation.isRestoreJob());
        Assertions.assertTrue(jobImmutableInformation.isCheckpointRestore());
        Assertions.assertFalse(jobImmutableInformation.isSavepointRestore());
        Assertions.assertFalse(jobImmutableInformation.isStartWithSavePoint());
    }

    @Test
    void shouldReadLegacyPayloadWithUnsafeInput() throws Exception {
        long jobId = 321L;
        byte[] payload =
                serializeLegacyPayload(
                        unsafeSerializationService, jobId, "unsafe-legacy-job", true, 654L);
        BufferObjectDataInput in = unsafeSerializationService.createObjectDataInput(payload);

        Assertions.assertTrue(in.getClass().getSimpleName().contains("UnsafeObjectDataInput"));

        JobImmutableInformation jobImmutableInformation = new JobImmutableInformation();
        jobImmutableInformation.readData(in);

        Assertions.assertEquals(RestoreMode.SAVEPOINT, jobImmutableInformation.getRestoreMode());
        Assertions.assertEquals(jobId, jobImmutableInformation.getRestoreSourceJobId());
    }

    private byte[] serializeLegacyPayload(
            long jobId, String jobName, boolean isStartWithSavePoint, long createTime)
            throws IOException {
        return serializeLegacyPayload(
                serializationService, jobId, jobName, isStartWithSavePoint, createTime);
    }

    private byte[] serializeLegacyPayload(
            InternalSerializationService service,
            long jobId,
            String jobName,
            boolean isStartWithSavePoint,
            long createTime)
            throws IOException {
        BufferObjectDataOutput out = service.createObjectDataOutput();
        out.writeLong(jobId);
        out.writeString(jobName);
        out.writeBoolean(isStartWithSavePoint);
        out.writeLong(createTime);
        out.writeInt(0);
        out.writeData(null);
        out.writeObject(null);
        out.writeObject(null);
        out.writeObject(null);
        return out.toByteArray();
    }

    private JobImmutableInformation deserializeCurrentPayload(byte[] payload) throws IOException {
        JobImmutableInformation jobImmutableInformation = new JobImmutableInformation();
        jobImmutableInformation.readData(serializationService.createObjectDataInput(payload));
        return jobImmutableInformation;
    }

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = JobImmutableInformation.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private boolean hasRemainingBytes(BufferObjectDataInput in) throws Exception {
        Method method = in.getClass().getMethod("available");
        method.setAccessible(true);
        return (Integer) method.invoke(in) > 0;
    }
}
