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

package org.apache.seatunnel.engine.server.trace;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/** Verifies row option helpers for adding, removing, and extending stain trace payloads. */
public class StainTraceUtilsTest {

    @Test
    void testSetPayloadDoesNotCopyHashMapOptions() {
        SeaTunnelRow row = new SeaTunnelRow(1);
        Map<String, Object> options = new HashMap<>();
        row.setOptions(options);
        StainTraceUtils.setPayload(row, new byte[] {1});
        Assertions.assertSame(options, row.getOptionsOrNull());
    }

    @Test
    void testSetPayloadCopiesNonHashMapOptions() {
        SeaTunnelRow row = new SeaTunnelRow(1);
        Map<String, Object> options = Collections.unmodifiableMap(new HashMap<>());
        row.setOptions(options);
        StainTraceUtils.setPayload(row, new byte[] {1});
        Assertions.assertNotSame(options, row.getOptionsOrNull());
    }

    @Test
    void testSetPayloadOnCopiedRowDoesNotMutateOriginalOptions() {
        SeaTunnelRow row = new SeaTunnelRow(1);
        Map<String, Object> options = new HashMap<>();
        options.put("business", "value");
        row.setOptions(options);

        SeaTunnelRow copied = row.copy();
        StainTraceUtils.setPayload(copied, new byte[] {1});

        Assertions.assertSame(options, row.getOptionsOrNull());
        Assertions.assertNotSame(row.getOptionsOrNull(), copied.getOptionsOrNull());
        Assertions.assertNull(
                row.getOptionsOrNull().get(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY));
        Assertions.assertArrayEquals(
                new byte[] {1},
                (byte[])
                        copied.getOptionsOrNull()
                                .get(StainTraceConstants.TRACE_PAYLOAD_OPTION_KEY));
    }
}
