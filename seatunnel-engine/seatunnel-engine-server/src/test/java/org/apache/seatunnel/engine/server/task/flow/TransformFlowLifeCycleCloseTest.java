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

package org.apache.seatunnel.engine.server.task.flow;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.transform.Collector;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.TransformChainAction;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.Collections;

/** Verifies the runtime's best-effort transform cleanup contract. */
class TransformFlowLifeCycleCloseTest {

    @Test
    void closeLogsTransformFailureAndContinuesCleanup() throws Exception {
        SeaTunnelTransform<SeaTunnelRow> failingTransform = Mockito.mock(SeaTunnelTransform.class);
        SeaTunnelTransform<SeaTunnelRow> followingTransform =
                Mockito.mock(SeaTunnelTransform.class);
        Mockito.when(failingTransform.getPluginName()).thenReturn("failing");
        Mockito.doThrow(new RuntimeException("close failed")).when(failingTransform).close();

        TransformChainAction<SeaTunnelRow> action =
                new TransformChainAction<>(
                        1L,
                        "close-test",
                        Collections.emptySet(),
                        Collections.emptySet(),
                        Arrays.asList(failingTransform, followingTransform));
        Collector<org.apache.seatunnel.api.table.type.Record<?>> collector =
                Mockito.mock(Collector.class);
        TransformFlowLifeCycle<SeaTunnelRow> flow =
                new TransformFlowLifeCycle<>(
                        action,
                        Mockito.mock(SeaTunnelTask.class),
                        collector,
                        new CompletableFuture<>());

        Assertions.assertDoesNotThrow(flow::close);
        Mockito.verify(failingTransform).close();
        Mockito.verify(followingTransform).close();
    }
}
