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

import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.config.server.scheduler.WaitConfig;
import org.apache.seatunnel.engine.common.config.server.scheduler.WaitRescheduleConfig;
import org.apache.seatunnel.engine.common.config.server.scheduler.WindowScanAgingPriorityConfig;

public final class PendingJobSchedulePolicyFactory {

    private PendingJobSchedulePolicyFactory() {}

    public static PendingJobSchedulePolicy create(ScheduleStrategy scheduleStrategy) {
        switch (scheduleStrategy) {
            case WAIT:
                return new WaitPolicy();
            case WAIT_RESCHEDULE:
                return new WaitReschedulePolicy();
            case WINDOW_SCAN_AGING_PRIORITY:
                return new WindowScanAgingPriorityPolicy();
            case REJECT:
            default:
                return new RejectPolicy();
        }
    }

    private static class WaitPolicy implements PendingJobSchedulePolicy {
        @Override
        public void onResourcesNotEnough(PendingJobScheduleContext context)
                throws InterruptedException {
            WaitConfig config =
                    context.getEngineConfig()
                            .getScheduleStrategyConfig(ScheduleStrategy.WAIT, WaitConfig.class);
            if (config == null) {
                config = new WaitConfig();
            }
            context.sleep(config.getSleepIntervalMillis());
        }
    }

    private static class WaitReschedulePolicy implements PendingJobSchedulePolicy {
        @Override
        public void onResourcesNotEnough(PendingJobScheduleContext context)
                throws InterruptedException {
            WaitRescheduleConfig config =
                    context.getEngineConfig()
                            .getScheduleStrategyConfig(
                                    ScheduleStrategy.WAIT_RESCHEDULE, WaitRescheduleConfig.class);
            if (config == null) {
                config = new WaitRescheduleConfig();
            }
            int maxRetryTimes = config.getMaxRetryTimes();
            int checkTimes = context.getPendingJobInfo().getCheckTimes();
            if (maxRetryTimes > 0
                    && context.getPendingJobQueue().size() > 1
                    && checkTimes > 0
                    && checkTimes % maxRetryTimes == 0) {
                context.moveHeadToTail();
            }
            context.sleep(config.getSleepIntervalMillis());
        }
    }

    private static class RejectPolicy implements PendingJobSchedulePolicy {
        @Override
        public void onResourcesNotEnough(PendingJobScheduleContext context) {
            context.failJob();
        }
    }

    private static class WindowScanAgingPriorityPolicy implements PendingJobSchedulePolicy {
        @Override
        public void onResourcesNotEnough(PendingJobScheduleContext context)
                throws InterruptedException {
            WindowScanAgingPriorityConfig config =
                    context.getEngineConfig()
                            .getScheduleStrategyConfig(
                                    ScheduleStrategy.WINDOW_SCAN_AGING_PRIORITY,
                                    WindowScanAgingPriorityConfig.class);
            if (config == null) {
                config = new WindowScanAgingPriorityConfig();
            }

            long ageMillis =
                    System.currentTimeMillis() - context.getPendingJobInfo().getEnqueueTimestamp();
            if (ageMillis >= config.getAgingThresholdMillis()) {
                context.sleep(config.getSleepIntervalMillis());
                return;
            }

            int checkTimes = context.getPendingJobInfo().getCheckTimes();
            int windowSize = config.getWindowSize();
            if (windowSize > 0
                    && context.getPendingJobQueue().size() > 1
                    && (checkTimes <= 0 || checkTimes % windowSize != 0)) {
                context.moveHeadToTail();
                context.sleep(0);
                return;
            }

            context.sleep(config.getSleepIntervalMillis());
        }
    }
}
