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
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.config.server.scheduler.WindowScanAgingPriorityConfig;
import org.apache.seatunnel.engine.server.diagnostic.PendingJobDiagnostic;
import org.apache.seatunnel.engine.server.execution.PendingJobInfo;
import org.apache.seatunnel.engine.server.utils.PeekBlockingQueue;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class WindowScanAgingPriorityPolicyTest {

    @Test
    public void testRotateWhenNotAged() throws Exception {
        EngineConfig engineConfig = new EngineConfig();
        engineConfig.setScheduleStrategy(ScheduleStrategy.WINDOW_SCAN_AGING_PRIORITY);
        WindowScanAgingPriorityConfig config = new WindowScanAgingPriorityConfig();
        config.setWindowSize(5);
        config.setAgingThresholdMillis(60000);
        config.setSleepIntervalMillis(3000);
        engineConfig.putScheduleStrategyConfig(ScheduleStrategy.WINDOW_SCAN_AGING_PRIORITY, config);

        PendingJobInfo pendingJobInfo = new PendingJobInfo(null, null);
        PendingJobDiagnostic snapshot = new PendingJobDiagnostic();
        snapshot.setCheckTime(System.currentTimeMillis());
        pendingJobInfo.recordSnapshot(snapshot);

        FakePeekBlockingQueue pendingJobQueue = new FakePeekBlockingQueue(2);
        FakeContext context =
                new FakeContext(pendingJobInfo, pendingJobQueue, engineConfig, 1L, () -> {});

        PendingJobSchedulePolicy policy =
                PendingJobSchedulePolicyFactory.create(ScheduleStrategy.WINDOW_SCAN_AGING_PRIORITY);
        policy.onResourcesNotEnough(context);

        Assertions.assertEquals(1, pendingJobQueue.moveToTailCount.get());
        Assertions.assertEquals(0, context.lastSleepMillis.get());
        Assertions.assertEquals(0, context.failCount.get());
    }

    @Test
    public void testDoNotRotateWhenAged() throws Exception {
        EngineConfig engineConfig = new EngineConfig();
        engineConfig.setScheduleStrategy(ScheduleStrategy.WINDOW_SCAN_AGING_PRIORITY);
        WindowScanAgingPriorityConfig config = new WindowScanAgingPriorityConfig();
        config.setWindowSize(5);
        config.setAgingThresholdMillis(1);
        config.setSleepIntervalMillis(1234);
        engineConfig.putScheduleStrategyConfig(ScheduleStrategy.WINDOW_SCAN_AGING_PRIORITY, config);

        PendingJobInfo pendingJobInfo = new PendingJobInfo(null, null);
        PendingJobDiagnostic snapshot = new PendingJobDiagnostic();
        snapshot.setCheckTime(System.currentTimeMillis());
        pendingJobInfo.recordSnapshot(snapshot);
        Thread.sleep(2);

        FakePeekBlockingQueue pendingJobQueue = new FakePeekBlockingQueue(2);
        FakeContext context =
                new FakeContext(pendingJobInfo, pendingJobQueue, engineConfig, 1L, () -> {});

        PendingJobSchedulePolicy policy =
                PendingJobSchedulePolicyFactory.create(ScheduleStrategy.WINDOW_SCAN_AGING_PRIORITY);
        policy.onResourcesNotEnough(context);

        Assertions.assertEquals(0, pendingJobQueue.moveToTailCount.get());
        Assertions.assertEquals(1234, context.lastSleepMillis.get());
        Assertions.assertEquals(0, context.failCount.get());
    }

    private static class FakePeekBlockingQueue extends PeekBlockingQueue<PendingJobInfo> {
        private final int fixedSize;
        private final AtomicInteger moveToTailCount = new AtomicInteger();

        FakePeekBlockingQueue(int fixedSize) {
            super(e -> 0L);
            this.fixedSize = fixedSize;
        }

        @Override
        public Integer size() {
            return fixedSize;
        }

        @Override
        public boolean moveToTail(Long jobId) {
            moveToTailCount.incrementAndGet();
            return true;
        }
    }

    private static class FakeContext implements PendingJobScheduleContext {
        private final PendingJobInfo pendingJobInfo;
        private final PeekBlockingQueue<PendingJobInfo> pendingJobQueue;
        private final EngineConfig engineConfig;
        private final long jobId;
        private final Runnable failJobAction;
        private final AtomicInteger failCount = new AtomicInteger();
        private final AtomicLong lastSleepMillis = new AtomicLong(-1);

        FakeContext(
                PendingJobInfo pendingJobInfo,
                PeekBlockingQueue<PendingJobInfo> pendingJobQueue,
                EngineConfig engineConfig,
                long jobId,
                Runnable failJobAction) {
            this.pendingJobInfo = pendingJobInfo;
            this.pendingJobQueue = pendingJobQueue;
            this.engineConfig = engineConfig;
            this.jobId = jobId;
            this.failJobAction = failJobAction;
        }

        @Override
        public PendingJobInfo getPendingJobInfo() {
            return pendingJobInfo;
        }

        @Override
        public PeekBlockingQueue<PendingJobInfo> getPendingJobQueue() {
            return pendingJobQueue;
        }

        @Override
        public EngineConfig getEngineConfig() {
            return engineConfig;
        }

        @Override
        public long getJobId() {
            return jobId;
        }

        @Override
        public boolean moveHeadToTail() {
            return pendingJobQueue.moveToTail(jobId);
        }

        @Override
        public void failJob() {
            failCount.incrementAndGet();
            failJobAction.run();
        }

        @Override
        public void sleep(long sleepMillis) {
            lastSleepMillis.set(sleepMillis);
        }
    }
}
