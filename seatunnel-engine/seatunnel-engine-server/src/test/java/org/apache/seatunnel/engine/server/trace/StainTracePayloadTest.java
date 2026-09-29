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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Verifies payload encoding, append behavior, and decoding for stain trace bytes. */
public class StainTracePayloadTest {

    @Test
    void testInitAndReadTraceId() {
        byte[] payload = StainTracePayload.init(123L, 456L);
        Assertions.assertTrue(StainTracePayload.isValid(payload));
        Assertions.assertEquals(123L, StainTracePayload.readTraceId(payload));
    }

    @Test
    void testAppendAndTruncate() {
        byte[] payload = StainTracePayload.init(1L, 2L);
        for (int i = 0; i < 3; i++) {
            StainTracePayload.AppendResult result =
                    StainTracePayload.append(payload, StainTraceStage.QUEUE_IN, 10L, 20L, 3);
            Assertions.assertEquals(StainTracePayload.AppendStatus.APPENDED, result.getStatus());
            payload = result.getPayload();
            Assertions.assertTrue(StainTracePayload.isValid(payload));
        }

        StainTracePayload.AppendResult truncated =
                StainTracePayload.append(payload, StainTraceStage.QUEUE_OUT, 10L, 20L, 3);
        Assertions.assertEquals(StainTracePayload.AppendStatus.TRUNCATED, truncated.getStatus());
        Assertions.assertSame(payload, truncated.getPayload());
    }
}
