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

package org.apache.seatunnel.engine.imap.storage.file.disruptor;

import com.lmax.disruptor.WorkHandler;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * Test handler that holds the sole WAL consumer until {@link #release()} so the ring can saturate.
 */
public final class BlockingWalWorkHandler implements WorkHandler<FileWALEvent> {

    private final WALWorkHandler delegate;
    private final CountDownLatch holdUntilRelease;

    public BlockingWalWorkHandler(WALWorkHandler delegate) {
        this.delegate = delegate;
        this.holdUntilRelease = new CountDownLatch(1);
    }

    public void release() {
        holdUntilRelease.countDown();
    }

    @Override
    public void onEvent(FileWALEvent event) throws Exception {
        if (event.getType() == WALEventType.APPEND) {
            holdUntilRelease.await(1, TimeUnit.HOURS);
        }
        delegate.onEvent(event);
    }
}
