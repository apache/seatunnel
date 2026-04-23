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

package org.apache.seatunnel.engine.server.task.context;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;

class SinkWriterContextTest {

    @Test
    void flushActionIsNullByDefault() {
        SinkWriterContext ctx = new SinkWriterContext(1, 0, null, null);
        Assertions.assertNull(
                ctx.getFlushAction(),
                "writer that did not opt in should expose a null flush action");
    }

    @Test
    void registerFlushActionStoresLastRegistered() throws Exception {
        SinkWriterContext ctx = new SinkWriterContext(2, 0, null, null);
        AtomicInteger first = new AtomicInteger();
        AtomicInteger second = new AtomicInteger();

        ctx.registerFlushAction(first::incrementAndGet);
        Assertions.assertSame(ctx.getFlushAction(), ctx.getFlushAction());
        ctx.getFlushAction().run();
        Assertions.assertEquals(1, first.get());

        ctx.registerFlushAction(second::incrementAndGet);
        ctx.getFlushAction().run();
        Assertions.assertEquals(1, first.get(), "old action must not be invoked after replace");
        Assertions.assertEquals(1, second.get());
    }

    @Test
    void registerFlushActionRejectsNull() {
        SinkWriterContext ctx = new SinkWriterContext(1, 0, null, null);
        Assertions.assertThrows(NullPointerException.class, () -> ctx.registerFlushAction(null));
    }
}
