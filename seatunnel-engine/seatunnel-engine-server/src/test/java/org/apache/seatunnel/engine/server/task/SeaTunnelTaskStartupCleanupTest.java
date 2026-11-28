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

package org.apache.seatunnel.engine.server.task;

import org.apache.seatunnel.api.transform.TransformOpenException;
import org.apache.seatunnel.engine.server.task.flow.FlowLifeCycle;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

/**
 * Verifies fatal transform initialization releases previously opened task flows.
 *
 * <p>Later task cleanup must not close the same flow again.
 */
class SeaTunnelTaskStartupCleanupTest {
    @Test
    void fatalTransformOpenClosesEarlierFlowsWithoutClosingThemAgain() throws Exception {
        FlowLifeCycle first = Mockito.mock(FlowLifeCycle.class);
        FlowLifeCycle second = Mockito.mock(FlowLifeCycle.class);
        FlowLifeCycle failing = Mockito.mock(FlowLifeCycle.class);
        FlowLifeCycle notOpened = Mockito.mock(FlowLifeCycle.class);
        IOException closeFailure = new IOException("close failure");
        Mockito.doThrow(closeFailure).when(second).close();
        TransformOpenException failure =
                new TransformOpenException("startup", new IllegalStateException());
        Mockito.doThrow(failure).when(failing).open();
        TransformSeaTunnelTask task =
                new TransformSeaTunnelTask(1L, null, 0, null, Collections.emptyMap());
        task.allCycles = Arrays.asList(first, second, failing, notOpened);
        Assertions.assertSame(
                failure,
                Assertions.assertThrows(TransformOpenException.class, task::openFlowLifeCycles));
        Assertions.assertArrayEquals(new Throwable[] {closeFailure}, failure.getSuppressed());
        Mockito.verify(first).close();
        Mockito.verify(second).close();
        Mockito.verify(notOpened, Mockito.never()).open();
        task.close();
        Mockito.verify(first, Mockito.times(1)).close();
        Mockito.verify(second, Mockito.times(1)).close();
    }
}
