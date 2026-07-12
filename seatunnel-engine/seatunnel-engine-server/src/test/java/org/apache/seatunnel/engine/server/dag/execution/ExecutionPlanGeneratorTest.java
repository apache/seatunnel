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

package org.apache.seatunnel.engine.server.dag.execution;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.engine.common.utils.IdGenerator;
import org.apache.seatunnel.engine.core.dag.actions.TransformAction;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class ExecutionPlanGeneratorTest {

    @Test
    public void testComputeChainedTransformVertexIdUsesMinIdAndNotAdvanceGenerator() {
        IdGenerator idGenerator = new IdGenerator();

        SeaTunnelTransform<?> dummyTransform =
                new SeaTunnelTransform<Object>() {
                    @Override
                    public CatalogTable getProducedCatalogTable() {
                        return null;
                    }

                    @Override
                    public List<CatalogTable> getProducedCatalogTables() {
                        return Collections.emptyList();
                    }

                    @Override
                    public String getPluginName() {
                        return "dummy";
                    }
                };

        ExecutionVertex v10 =
                new ExecutionVertex(
                        10L,
                        new TransformAction(
                                10L,
                                "t1",
                                (SeaTunnelTransform<?>) dummyTransform,
                                Collections.emptySet(),
                                Collections.<ConnectorJarIdentifier>emptySet()),
                        1);
        ExecutionVertex v8 =
                new ExecutionVertex(
                        8L,
                        new TransformAction(
                                8L,
                                "t2",
                                (SeaTunnelTransform<?>) dummyTransform,
                                Collections.emptySet(),
                                Collections.<ConnectorJarIdentifier>emptySet()),
                        1);
        List<ExecutionVertex> vertices = Arrays.asList(v10, v8);

        long id = ExecutionPlanGenerator.computeChainedTransformVertexId(vertices, idGenerator);
        Assertions.assertEquals(8L, id);

        // Should not have advanced the generator when min is present.
        Assertions.assertEquals(1L, idGenerator.getNextId());
    }

    @Test
    public void testComputeChainedTransformVertexIdUsesGeneratorWhenEmpty() {
        IdGenerator idGenerator = new IdGenerator();
        long id =
                ExecutionPlanGenerator.computeChainedTransformVertexId(
                        Collections.emptyList(), idGenerator);
        Assertions.assertEquals(1L, id);
    }
}
