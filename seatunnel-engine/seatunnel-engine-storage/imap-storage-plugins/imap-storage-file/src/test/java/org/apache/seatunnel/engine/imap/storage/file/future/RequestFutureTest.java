/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file.future;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

class RequestFutureTest {

    @Test
    void doneShouldPublishSuccessAndCompletion() throws Exception {
        RequestFuture future = new RequestFuture();
        Assertions.assertFalse(future.isDone());

        future.done(true);

        Assertions.assertTrue(future.isDone());
        Assertions.assertTrue(future.get());
        Assertions.assertTrue(future.get(1, TimeUnit.MILLISECONDS));
    }

    @Test
    void doneShouldPublishFailureWithoutTreatingCompletionAsSuccess() throws Exception {
        RequestFuture future = new RequestFuture();
        future.done(false);

        Assertions.assertTrue(future.isDone());
        Assertions.assertFalse(future.get());
    }

    @Test
    void timedGetShouldTimeoutWhileIncomplete() {
        RequestFuture future = new RequestFuture();
        Assertions.assertThrows(
                TimeoutException.class, () -> future.get(10, TimeUnit.MILLISECONDS));
        Assertions.assertFalse(future.isDone());
    }

    @Test
    void getShouldObserveCompletionPublishedByAnotherThread() throws Exception {
        RequestFuture future = new RequestFuture();
        AtomicBoolean published = new AtomicBoolean(false);
        Thread publisher =
                new Thread(
                        () -> {
                            published.set(true);
                            future.done(true);
                        });
        publisher.start();

        Assertions.assertTrue(future.get(2, TimeUnit.SECONDS));
        publisher.join(TimeUnit.SECONDS.toMillis(2));
        Assertions.assertTrue(published.get());
        Assertions.assertTrue(future.isDone());
    }
}
