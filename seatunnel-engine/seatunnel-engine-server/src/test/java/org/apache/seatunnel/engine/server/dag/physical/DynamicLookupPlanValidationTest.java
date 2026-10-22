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

package org.apache.seatunnel.engine.server.dag.physical;

import org.apache.seatunnel.engine.core.dag.actions.DynamicLookupAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.logical.ExchangeDescriptor;
import org.apache.seatunnel.engine.server.dag.execution.ExecutionVertex;
import org.apache.seatunnel.engine.server.dag.execution.PortAwareExecutionEdge;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;

/** Prevents programmatically built FORWARD lookup plans from bypassing key co-location checks. */
class DynamicLookupPlanValidationTest {

    @Test
    void rejectsParallelSourcesAndLookupEvenWhenTheirParallelismMatches() {
        for (int[] parallelism : new int[][] {{2, 2, 2}, {2, 1, 1}, {1, 2, 1}, {1, 1, 2}}) {
            DynamicLookupAction lookup = Mockito.mock(DynamicLookupAction.class);
            Mockito.when(lookup.getParallelism()).thenReturn(parallelism[2]);
            ExecutionVertex target = new ExecutionVertex(3L, lookup, parallelism[2]);
            ExecutionVertex fact =
                    new ExecutionVertex(1L, Mockito.mock(SourceAction.class), parallelism[0]);
            ExecutionVertex dimension =
                    new ExecutionVertex(2L, Mockito.mock(SourceAction.class), parallelism[1]);
            IllegalArgumentException error =
                    Assertions.assertThrows(
                            IllegalArgumentException.class,
                            () ->
                                    PhysicalPlanGenerator.validateDynamicLookupInputEdges(
                                            lookup,
                                            Arrays.asList(
                                                    new PortAwareExecutionEdge(
                                                            fact,
                                                            target,
                                                            10L,
                                                            0,
                                                            ExchangeDescriptor.forward()),
                                                    new PortAwareExecutionEdge(
                                                            dimension,
                                                            target,
                                                            11L,
                                                            1,
                                                            ExchangeDescriptor.forward()))));
            Assertions.assertTrue(
                    error.getMessage().contains("parallelism = 1"), error.getMessage());
        }
    }
}
