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

package org.apache.seatunnel.engine.server.execution;

import org.apache.seatunnel.common.utils.ExceptionUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.NonNull;

import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

/** For test use, only print logs */
public class TestTask implements Task {

    private static final Logger logger = LoggerFactory.getLogger(TestTask.class);

    private final AtomicBoolean stop;
    private final long sleep;
    private final boolean isThreadsShare;
    private final long taskId;

    public TestTask(AtomicBoolean stop, long sleep, boolean isThreadsShare) {
        this.stop = stop;
        this.sleep = sleep;
        this.isThreadsShare = isThreadsShare;
        this.taskId = new Random().nextInt();
    }

    @NonNull @Override
    public ProgressState call() {
        ProgressState progressState;
        if (!stop.get()) {
            logger.info("TestTask is running.........");
            try {
                Thread.sleep(sleep);
            } catch (InterruptedException e) {
                logger.error(ExceptionUtils.getMessage(e));
            }
            progressState = ProgressState.MADE_PROGRESS;
        } else {
            progressState = ProgressState.DONE;
        }
        return progressState;
    }

    @NonNull @Override
    public Long getTaskID() {
        return taskId;
    }

    @Override
    public boolean isThreadsShare() {
        return isThreadsShare;
    }
}
