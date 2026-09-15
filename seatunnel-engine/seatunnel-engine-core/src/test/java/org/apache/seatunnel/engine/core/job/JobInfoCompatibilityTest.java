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

import org.junit.jupiter.api.Test;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.nio.IOUtil;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class JobInfoCompatibilityTest {
    @Test
    void readsLegacyMetadataAndPreservesOrderedAdmissionsWithBothInputs() throws Exception {
        for (boolean unsafe : new boolean[] {false, true}) {
            InternalSerializationService service =
                    (InternalSerializationService)
                            new DefaultSerializationServiceBuilder()
                                    .setAllowUnsafe(unsafe)
                                    .setUseNativeByteOrder(unsafe)
                                    .setByteOrder(
                                            unsafe ? ByteOrder.nativeOrder() : ByteOrder.BIG_ENDIAN)
                                    .build();
            try {
                Data immutableInfo = service.toData("serialized-job");
                BufferObjectDataOutput legacy = service.createObjectDataOutput();
                legacy.writeLong(123L);
                IOUtil.writeData(legacy, immutableInfo);
                JobInfo restored = read(service, legacy.toByteArray());
                assertEquals(123L, restored.getInitializationTimestamp());
                assertEquals(immutableInfo, restored.getJobImmutableInformation());
                assertEquals(0L, restored.getEnqueueSequence());

                JobInfo ordered = new JobInfo(123L, immutableInfo);
                ordered.setEnqueueSequence(42L);
                BufferObjectDataOutput current = service.createObjectDataOutput();
                ordered.writeData(current);
                // The original fields remain a byte-for-byte prefix for older readers.
                assertArrayEquals(
                        legacy.toByteArray(),
                        Arrays.copyOf(current.toByteArray(), legacy.toByteArray().length));
                JobInfo roundTrip = read(service, current.toByteArray());
                assertEquals(ordered, roundTrip);
                assertEquals(ordered, service.toObject(service.toData(ordered)));

                byte[] truncated =
                        Arrays.copyOf(current.toByteArray(), current.toByteArray().length - 1);
                assertThrows(IOException.class, () -> read(service, truncated));
            } finally {
                service.dispose();
            }
        }
    }

    private JobInfo read(InternalSerializationService service, byte[] bytes) throws IOException {
        BufferObjectDataInput input = service.createObjectDataInput(bytes);
        JobInfo info = new JobInfo();
        info.readData(input);
        return info;
    }
}
