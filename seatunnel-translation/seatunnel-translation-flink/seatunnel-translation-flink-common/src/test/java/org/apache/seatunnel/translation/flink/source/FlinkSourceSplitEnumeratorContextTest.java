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

package org.apache.seatunnel.translation.flink.source;

import org.apache.seatunnel.api.source.SourceSplit;

import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Regression for issue #10193: resolving Flink job id must not abort enumerator construction during
 * checkpoint/savepoint restore when Flink internals are unavailable.
 */
class FlinkSourceSplitEnumeratorContextTest {

    private static final class DummySplit implements SourceSplit {
        private static final long serialVersionUID = 1L;

        @Override
        public String splitId() {
            return "dummy";
        }
    }

    @Test
    void constructContextWhenJobIdCannotBeResolved() {
        @SuppressWarnings("unchecked")
        SplitEnumeratorContext<SplitWrapper<DummySplit>> enumContext =
                Mockito.mock(SplitEnumeratorContext.class);
        Mockito.when(enumContext.currentParallelism()).thenReturn(1);
        Mockito.when(enumContext.registeredReaders()).thenReturn(Collections.emptyMap());

        AtomicBoolean signaled = new AtomicBoolean(false);
        FlinkSourceSplitEnumeratorContext<DummySplit> context =
                Assertions.assertDoesNotThrow(
                        () ->
                                new FlinkSourceSplitEnumeratorContext<>(
                                        enumContext, id -> signaled.set(true)));

        Assertions.assertNotNull(context.getEventListener());
        Assertions.assertEquals(1, context.currentParallelism());
        Assertions.assertTrue(context.registeredReaders().isEmpty());

        context.signalNoMoreSplits(0);
        Assertions.assertTrue(signaled.get());
        Mockito.verify(enumContext).signalNoMoreSplits(0);
    }
}
