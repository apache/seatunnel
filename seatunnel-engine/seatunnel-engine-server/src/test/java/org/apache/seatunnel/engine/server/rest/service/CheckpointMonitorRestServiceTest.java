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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.engine.core.checkpoint.CheckpointOverview;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for the {@code /jobs/checkpoints/{jobId}} response envelope.
 *
 * <p>The field under test is {@code generatedAt}. It exists because a client cannot compute the age
 * of an in-progress checkpoint from {@code updatedAt}: that is the time of the last recorded
 * checkpoint event, so while a checkpoint hangs waiting for its last subtask it stops advancing,
 * and the stall becomes invisible to anything subtracting a trigger timestamp from it.
 */
public class CheckpointMonitorRestServiceTest {

    private static final long JOB_ID = 852L;

    /**
     * Overrides the server lookup so the response can be built without a running cluster. {@code
     * getSeaTunnelServer} is protected for this reason; {@code getMonitorService} below it is
     * private and reaches the mock through it.
     */
    private static CheckpointMonitorRestService serviceReturning(
            CheckpointMonitorService monitorService) {
        SeaTunnelServer server = mock(SeaTunnelServer.class);
        when(server.getCheckpointMonitorService()).thenReturn(monitorService);
        return new CheckpointMonitorRestService(mock(NodeEngineImpl.class)) {
            @Override
            protected SeaTunnelServer getSeaTunnelServer(boolean shouldBeMaster) {
                return server;
            }
        };
    }

    @Test
    public void testGeneratedAtIsStampedAtResponseTime() {
        CheckpointOverview overview = new CheckpointOverview(JOB_ID);
        overview.setUpdatedAt(1_000L);
        CheckpointMonitorService monitorService = mock(CheckpointMonitorService.class);
        when(monitorService.getOverview(JOB_ID)).thenReturn(Optional.of(overview));

        long before = System.currentTimeMillis();
        JsonObject result = serviceReturning(monitorService).getOverview(JOB_ID);
        long after = System.currentTimeMillis();

        long generatedAt = result.getLong("generatedAt", -1L);
        Assertions.assertTrue(
                generatedAt >= before && generatedAt <= after,
                "generatedAt should be the response time, but was " + generatedAt);

        // And it must not be confused with updatedAt, which the engine only
        // advances when a checkpoint event is recorded.
        Assertions.assertEquals(1_000L, result.getLong("updatedAt", -1L));
        Assertions.assertNotEquals(result.getLong("updatedAt", -1L), generatedAt);
    }

    @Test
    public void testGeneratedAtIsPresentWhenThereIsNoOverview() {
        // A job the monitor has nothing recorded for: the response carries no
        // pipelines, and a client still needs to know it came from a live
        // server rather than a stale cache.
        CheckpointMonitorService monitorService = mock(CheckpointMonitorService.class);
        when(monitorService.getOverview(JOB_ID)).thenReturn(Optional.empty());

        JsonObject result = serviceReturning(monitorService).getOverview(JOB_ID);

        Assertions.assertTrue(result.getLong("generatedAt", -1L) > 0L);
        Assertions.assertNull(result.get("pipelines"));
    }

    @Test
    public void testGeneratedAtIsPresentWhenTheMonitorServiceIsOff() {
        // enable-http without the monitor service running: getMonitorService()
        // returns null and the method returns early, before the branch that
        // adds updatedAt and pipelines.
        JsonObject result = serviceReturning(null).getOverview(JOB_ID);

        Assertions.assertTrue(result.getLong("generatedAt", -1L) > 0L);
        Assertions.assertEquals(String.valueOf(JOB_ID), result.getString("jobId", null));
        Assertions.assertNull(result.get("updatedAt"));
    }
}
