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

package org.apache.seatunnel.engine.core.dag.logical;

import org.apache.seatunnel.engine.core.dag.actions.InputPortBinding;
import org.apache.seatunnel.engine.core.serializable.JobDataSerializerHook;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;

import java.io.IOException;
import java.util.LinkedHashSet;
import java.util.Set;

/** Verifies that port-aware edge serialization does not change the legacy edge wire contract. */
class LogicalEdgeSerializationCompatibilityTest {

    private final InternalSerializationService serializationService =
            new DefaultSerializationServiceBuilder().build();

    @Test
    void legacyLogicalEdgePayloadRemainsTwoLongs() throws IOException {
        LogicalEdge edge = new LogicalEdge(11L, 22L);
        BufferObjectDataOutput output = serializationService.createObjectDataOutput();

        edge.writeData(output);

        // Captured from LogicalEdge.writeData at baseline 924b289e95: two big-endian longs.
        byte[] baselineFixture =
                new byte[] {
                    0, 0, 0, 0, 0, 0, 0, 11,
                    0, 0, 0, 0, 0, 0, 0, 22
                };
        Assertions.assertArrayEquals(baselineFixture, output.toByteArray());
        Assertions.assertEquals(JobDataSerializerHook.LOGICAL_EDGE, edge.getClassId());

        LogicalEdge restored = new LogicalEdge();
        restored.readData(serializationService.createObjectDataInput(baselineFixture));
        Assertions.assertEquals(edge, restored);
    }

    @Test
    void portAwareLogicalEdgeUsesDedicatedTaggedSerialization() {
        PortAwareLogicalEdge edge =
                new PortAwareLogicalEdge(31L, 11L, 22L, 1, ExchangeDescriptor.forward());

        PortAwareLogicalEdge restored =
                serializationService.toObject(serializationService.toData(edge));

        Assertions.assertEquals(JobDataSerializerHook.PORT_AWARE_LOGICAL_EDGE, edge.getClassId());
        Assertions.assertEquals(edge, restored);
        Assertions.assertEquals(
                PortAwareLogicalEdge.CURRENT_FORMAT_VERSION, restored.getEdgeFormatVersion());
        Assertions.assertArrayEquals(
                edge.getExchangeDescriptor().toCanonicalBytes(),
                restored.getExchangeDescriptor().toCanonicalBytes());
    }

    @Test
    void exchangeAndEdgeIdentitiesHaveGoldenVectors() {
        Assertions.assertArrayEquals(
                new byte[] {0, 0, 0, 1, 0, 0, 0, 0},
                ExchangeDescriptor.forward().toCanonicalBytes());
        Assertions.assertEquals(
                2584707732897459687L, InputPortBinding.forward(11L, 22L, 1).getEdgeId());
    }

    @Test
    void portAwareLogicalEdgeRejectsUnknownFormatVersion() throws IOException {
        BufferObjectDataOutput output = serializationService.createObjectDataOutput();
        output.writeInt(PortAwareLogicalEdge.CURRENT_FORMAT_VERSION + 1);
        BufferObjectDataInput input =
                serializationService.createObjectDataInput(output.toByteArray());

        IOException error =
                Assertions.assertThrows(
                        IOException.class, () -> new PortAwareLogicalEdge().readData(input));

        Assertions.assertTrue(
                error.getMessage().contains("Unsupported port-aware logical edge version"));
    }

    @Test
    void legacyAndPortAwareEdgesHaveSymmetricDistinctIdentity() {
        LogicalEdge legacy = new LogicalEdge(11L, 22L);
        PortAwareLogicalEdge fact =
                new PortAwareLogicalEdge(31L, 11L, 22L, 0, ExchangeDescriptor.forward());
        PortAwareLogicalEdge dimension =
                new PortAwareLogicalEdge(32L, 11L, 22L, 1, ExchangeDescriptor.forward());
        Set<LogicalEdge> edges = new LinkedHashSet<>();

        edges.add(legacy);
        edges.add(fact);
        edges.add(dimension);

        Assertions.assertFalse(legacy.equals(fact));
        Assertions.assertFalse(fact.equals(legacy));
        Assertions.assertEquals(3, edges.size());
    }

    @Test
    void logicalDagRejectsReusedEdgeIdWithDifferentDescriptor() {
        LogicalDag dag = new LogicalDag();
        dag.addEdge(new PortAwareLogicalEdge(31L, 11L, 22L, 0, ExchangeDescriptor.forward()));

        IllegalArgumentException error =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                dag.addEdge(
                                        new PortAwareLogicalEdge(
                                                31L, 11L, 22L, 1, ExchangeDescriptor.forward())));

        Assertions.assertTrue(error.getMessage().contains("EDGE_IDENTITY_COLLISION"));
    }
}
