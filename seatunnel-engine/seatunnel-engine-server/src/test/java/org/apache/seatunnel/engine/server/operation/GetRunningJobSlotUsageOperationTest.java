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

package org.apache.seatunnel.engine.server.operation;

import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineRetryableException;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/** Tests response waiting behavior for the running-job slot usage operation. */
class GetRunningJobSlotUsageOperationTest {

    @Test
    void shouldUnwrapRetryableExceptionsFromAsyncExecution() {
        CompletableFuture<String> future = new CompletableFuture<>();
        future.completeExceptionally(new SeaTunnelEngineRetryableException("retryable"));

        SeaTunnelEngineRetryableException exception =
                Assertions.assertThrows(
                        SeaTunnelEngineRetryableException.class,
                        () -> GetRunningJobSlotUsageOperation.awaitResponse(future));
        Assertions.assertEquals("retryable", exception.getMessage());
    }

    @Test
    void shouldReturnSuccessfulResponse() {
        CompletableFuture<String> future = new CompletableFuture<>();
        future.complete("ok");

        Assertions.assertEquals("ok", GetRunningJobSlotUsageOperation.awaitResponse(future));
    }
}
