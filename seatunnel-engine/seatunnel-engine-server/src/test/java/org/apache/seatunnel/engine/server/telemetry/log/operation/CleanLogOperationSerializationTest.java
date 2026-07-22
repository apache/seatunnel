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

package org.apache.seatunnel.engine.server.telemetry.log.operation;

import org.apache.seatunnel.common.utils.ReflectionUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies that CleanLogOperation correctly preserves jobId through Hazelcast serialization
 * round-trip (writeInternal / readInternal).
 *
 * <p>This is critical in split-deployment mode where the operation is serialized on the master node
 * and deserialized on the worker node. Without proper writeInternal/readInternal, the worker
 * receives jobId=0.
 */
public class CleanLogOperationSerializationTest {

    private InternalSerializationService serializationService;

    @BeforeEach
    void setUp() {
        serializationService = new DefaultSerializationServiceBuilder().build();
    }

    @Test
    void testJobIdIsPreservedAfterSerialization() {
        long expectedJobId = 123456789L;

        CleanLogOperation original = new CleanLogOperation(expectedJobId);
        Data data = serializationService.toData(original);

        CleanLogOperation deserialized = serializationService.toObject(data);
        Long jobId =
                ReflectionUtils.getField(deserialized, "jobId")
                        .map(field -> (long) field)
                        .orElseThrow(
                                () -> new RuntimeException("Failed to read jobId via reflection"));

        assertEquals(expectedJobId, jobId);
    }
}
