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

package org.apache.seatunnel.engine.server.scheduler;

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.server.execution.PendingJobInfo;
import org.apache.seatunnel.engine.server.utils.PeekBlockingQueue;

import lombok.Value;

@Value
public class PendingJobScheduleContext {
    PendingJobInfo pendingJobInfo;
    PeekBlockingQueue<PendingJobInfo> pendingJobQueue;
    EngineConfig engineConfig;
    long jobId;
    Runnable failJobAction;

    public boolean moveHeadToTail() {
        return pendingJobQueue.moveToTail(jobId);
    }

    public void failJob() {
        failJobAction.run();
    }

    public void sleep(long sleepMillis) throws InterruptedException {
        if (sleepMillis > 0) {
            Thread.sleep(sleepMillis);
        }
    }
}
