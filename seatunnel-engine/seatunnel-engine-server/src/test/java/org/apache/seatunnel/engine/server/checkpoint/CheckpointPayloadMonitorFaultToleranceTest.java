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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.mockito.Mockito;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;

/**
 * Engine fault-tolerance coverage for payload vs monitor isolation: a real running job must keep
 * its durable checkpoint payload when auxiliary monitor cleanup fails on cancel.
 */
@DisabledOnOs(OS.WINDOWS)
public class CheckpointPayloadMonitorFaultToleranceTest extends AbstractSeaTunnelServerTest {

    private static final String STREAM_CONF_WITH_CHECKPOINT_PATH =
            "stream_fake_to_console_with_checkpoint.conf";

    @Override
    public SeaTunnelConfig loadSeaTunnelConfig() {
        SeaTunnelConfig seaTunnelConfig = super.loadSeaTunnelConfig();
        CheckpointConfig checkpointConfig = seaTunnelConfig.getEngineConfig().getCheckpointConfig();
        checkpointConfig.setRetainAfterJobCancelled(true);
        seaTunnelConfig.getEngineConfig().setCheckpointConfig(checkpointConfig);
        return seaTunnelConfig;
    }

    @Test
    public void canceledRunningJobRetainsCheckpointWhenMonitorCleanupFails() throws Exception {
        long jobId = System.currentTimeMillis();
        CheckpointStorage checkpointStorage = server.getCheckpointService().getCheckpointStorage();

        startJob(jobId, STREAM_CONF_WITH_CHECKPOINT_PATH, false);
        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.RUNNING,
                                        server.getCoordinatorService().getJobStatus(jobId)));

        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertFalse(
                                        checkpointStorage
                                                .getAllCheckpoints(String.valueOf(jobId))
                                                .isEmpty(),
                                        "the running job must persist at least one checkpoint"));

        CheckpointMonitorService realMonitor = server.getCheckpointMonitorService();
        CheckpointMonitorService spiedMonitor = Mockito.spy(realMonitor);
        doThrow(new IMapStorageException("overview WAL fail-closed"))
                .when(spiedMonitor)
                .cleanupJob(eq(jobId));
        JobMaster jobMaster = server.getCoordinatorService().getJobMaster(jobId);
        ReflectionUtils.setField(
                jobMaster.getCheckpointManager(), "checkpointMonitorService", spiedMonitor);
        server.getCoordinatorService().cancelJob(jobId);
        await().atMost(120, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        JobStatus.CANCELED,
                                        server.getCoordinatorService().getJobStatus(jobId)));

        Assertions.assertFalse(
                checkpointStorage.getAllCheckpoints(String.valueOf(jobId)).isEmpty(),
                "monitor cleanup failure must not delete the durable checkpoint payload when retain is enabled");
        Mockito.verify(spiedMonitor).cleanupJob(jobId);
    }
}
