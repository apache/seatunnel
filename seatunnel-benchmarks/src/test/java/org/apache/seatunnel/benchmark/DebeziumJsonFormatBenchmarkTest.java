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

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

class DebeziumJsonFormatBenchmarkTest {

    @Test
    void shouldKeepThreadScopedMutableSerializerState() {
        assertEquals(
                Scope.Thread, DebeziumJsonFormatBenchmark.class.getAnnotation(State.class).value());
    }

    @Test
    void shouldRunDebeziumJsonFormatBenchmarkMethods() {
        DebeziumJsonFormatBenchmark benchmark = new DebeziumJsonFormatBenchmark();
        benchmark.setUp();

        SeaTunnelRow insertRow = benchmark.deserializeInsertEvent();
        SeaTunnelRow updateAfterRow = benchmark.deserializeUpdateEvent();
        byte[] insertJson = benchmark.serializeInsertEvent();
        byte[] updateJson = benchmark.serializeMergedUpdateEvent();

        assertEquals(RowKind.INSERT, insertRow.getRowKind());
        assertEquals(6, insertRow.getArity());
        assertEquals(1001L, insertRow.getField(0));
        assertEquals("seatunnel-order", insertRow.getField(1));
        assertEquals(RowKind.UPDATE_AFTER, updateAfterRow.getRowKind());
        assertEquals(6, updateAfterRow.getArity());
        assertEquals(13.75D, updateAfterRow.getField(3));
        assertNotNull(insertJson);
        assertNotNull(updateJson);

        String insertEnvelope = new String(insertJson, StandardCharsets.UTF_8);
        String updateEnvelope = new String(updateJson, StandardCharsets.UTF_8);
        assertTrue(insertEnvelope.contains("\"op\":\"c\""));
        assertTrue(insertEnvelope.contains("\"after\":{"));
        assertTrue(insertEnvelope.contains("\"before\":null"));
        assertTrue(updateEnvelope.contains("\"op\":\"u\""));
        assertTrue(updateEnvelope.contains("\"before\":{"));
        assertTrue(updateEnvelope.contains("\"after\":{"));
        assertTrue(updateEnvelope.contains("12.5"));
        assertTrue(updateEnvelope.contains("13.75"));
        assertTrue(updateEnvelope.contains("199.99"));
        assertTrue(updateEnvelope.contains("249.5"));
    }
}
