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

package org.apache.seatunnel.benchmark;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SeaTunnelRowBenchmarkTest {

    @Test
    void shouldRunSeaTunnelRowBenchmarkMethods() throws Exception {
        SeaTunnelRowBenchmark benchmark = new SeaTunnelRowBenchmark();
        setRowCount(benchmark, 4);
        benchmark.setUp();

        SeaTunnelRow plainCopy = benchmark.copyPlainRow();
        SeaTunnelRow optionCopy = benchmark.copyRowWithOptions();
        SeaTunnelRow traceCopy = benchmark.copyRowWithTracePayload();
        SeaTunnelRow projectedCopy = benchmark.copyProjectedPlainRow();
        SeaTunnelRow projectedOptionCopy = benchmark.copyProjectedRowWithOptions();

        assertNotSame(plainCopy, benchmark.copyPlainRow());
        assertEquals(8, plainCopy.getArity());
        assertEquals(8, optionCopy.getArity());
        assertEquals(8, traceCopy.getArity());
        assertEquals(4, projectedCopy.getArity());
        assertEquals(4, projectedOptionCopy.getArity());
        assertEquals(4, benchmark.copyThenMutateCopiedOptions());
        assertTrue(benchmark.readFields() > 0);
        assertTrue(benchmark.getBytesSizeCached() > 0);
        assertTrue(benchmark.createRowAndGetBytesSize() > 0);
        assertEquals(8, benchmark.createRowWithSetField());
    }

    private static void setRowCount(SeaTunnelRowBenchmark benchmark, int rowCount)
            throws Exception {
        Field field = SeaTunnelRowBenchmark.class.getDeclaredField("rowCount");
        field.setAccessible(true);
        field.setInt(benchmark, rowCount);
    }
}
