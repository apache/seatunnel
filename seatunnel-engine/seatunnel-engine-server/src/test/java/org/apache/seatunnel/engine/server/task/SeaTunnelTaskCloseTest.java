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

import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.task.flow.FlowLifeCycle;

import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

class SeaTunnelTaskCloseTest {

    @Test
    void shouldCloseRemainingCyclesAfterUncheckedFailure() throws Exception {
        IllegalStateException failure = new IllegalStateException("first close failed");
        FlowLifeCycle first = mock(FlowLifeCycle.class);
        FlowLifeCycle second = mock(FlowLifeCycle.class);
        doThrow(failure).when(first).close();
        SeaTunnelTask task = taskWithCycles(first, second);

        IllegalStateException thrown = assertThrows(IllegalStateException.class, task::close);

        assertSame(failure, thrown);
        verify(second).close();
    }

    @Test
    void shouldPreserveFirstFailureAndSuppressLaterFailures() throws Exception {
        IOException firstFailure = new IOException("first close failed");
        IllegalStateException secondFailure = new IllegalStateException("second close failed");
        FlowLifeCycle first = mock(FlowLifeCycle.class);
        FlowLifeCycle second = mock(FlowLifeCycle.class);
        FlowLifeCycle third = mock(FlowLifeCycle.class);
        doThrow(firstFailure).when(first).close();
        doThrow(secondFailure).when(second).close();
        SeaTunnelTask task = taskWithCycles(first, second, third);

        IOException thrown = assertThrows(IOException.class, task::close);

        assertSame(firstFailure, thrown);
        assertSame(secondFailure, thrown.getSuppressed()[0]);
        verify(third).close();
    }

    @Test
    void shouldAttemptEveryCycleOnRepeatedClose() throws Exception {
        FlowLifeCycle first = mock(FlowLifeCycle.class);
        FlowLifeCycle second = mock(FlowLifeCycle.class);
        doThrow(new IllegalStateException("close failed")).when(first).close();
        SeaTunnelTask task = taskWithCycles(first, second);

        assertThrows(IllegalStateException.class, task::close);
        assertThrows(IllegalStateException.class, task::close);

        verify(first, times(2)).close();
        verify(second, times(2)).close();
    }

    private static SeaTunnelTask taskWithCycles(FlowLifeCycle... cycles) throws Exception {
        SeaTunnelTask task = mock(SeaTunnelTask.class, Mockito.CALLS_REAL_METHODS);
        setField(SeaTunnelTask.class, "allCycles", task, Arrays.asList(cycles));
        setField(AbstractTask.class, "restoreComplete", task, new CompletableFuture<>());
        return task;
    }

    private static void setField(Class<?> type, String name, Object target, Object value)
            throws Exception {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
