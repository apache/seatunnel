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

package org.apache.seatunnel.benchmark.transform;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.benchmark.connector.BenchmarkConnectorOptions;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;

class BenchmarkTransformTest {

    @Test
    void shouldApplyDeterministicCpuWorkAndPreserveInput() {
        BenchmarkTransform transform =
                new BenchmarkTransform(BenchmarkConnectorOptions.catalogTable(), 64, true);
        SeaTunnelRow input = new SeaTunnelRow(new Object[] {7L, 1_000L, "payload", 0L});

        SeaTunnelRow first = transform.map(input);
        SeaTunnelRow second = transform.map(input);

        assertNotSame(input, first);
        assertEquals(0L, input.getField(3));
        assertNotEquals(0L, first.getField(3));
        assertEquals(first.getField(3), second.getField(3));
        assertEquals(input.getField(0), first.getField(0));
        assertEquals(input.getField(1), first.getField(1));
        assertEquals(input.getField(2), first.getField(2));
    }
}
