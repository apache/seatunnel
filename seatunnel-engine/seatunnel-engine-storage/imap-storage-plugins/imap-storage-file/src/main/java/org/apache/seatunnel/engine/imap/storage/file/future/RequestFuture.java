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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public class RequestFuture implements Future<Boolean> {

    private CountDownLatch latch = new CountDownLatch(1);

    private boolean success = false;

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return false;
    }

    @Override
    public boolean isCancelled() {
        return false;
    }

    @Override
    public boolean isDone() {
        return success;
    }

    @Override
    public Boolean get() throws InterruptedException {
        if (success) {
            return true;
        }
        latch.await(1, TimeUnit.SECONDS);
        if (!success) {
            return false;
        }
        return success;
    }

    @Override
    public Boolean get(long timeout, TimeUnit unit) throws InterruptedException {
        if (success) {
            return true;
        }
        latch.await(timeout, unit);
        return success;
    }

    public void done(boolean success) {
        this.success = success;
        latch.countDown();
    }

}
