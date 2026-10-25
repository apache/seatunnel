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

package org.apache.seatunnel.engine.server.diagnostic;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.master.JobMaster;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.internal.json.JsonArray;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Arrays;
import java.util.Collections;

import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Covers the read-only job diagnostics payload exposed by the job-info REST endpoint. */
public class JobRuntimeDiagnosticsTest {

    private static final long JOB_ID = 862858126931165185L;

    private SeaTunnelServer server;
    private CoordinatorService coordinatorService;
    private IMap<Object, Long[]> stateTimestampsMap;

    @BeforeEach
    void setUp() {
        server = mock(SeaTunnelServer.class);
        coordinatorService = mock(CoordinatorService.class);
        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);
        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
        stateTimestampsMap = mock(IMap.class);

        when(server.getNodeEngine()).thenReturn(nodeEngine);
        when(server.getCoordinatorService()).thenReturn(coordinatorService);
        when(nodeEngine.getHazelcastInstance()).thenReturn(hazelcastInstance);
        doReturn(stateTimestampsMap).when(hazelcastInstance).getMap(Constant.IMAP_STATE_TIMESTAMPS);
    }

    private Long[] jobTimestamps() {
        Long[] timestamps = new Long[JobStatus.values().length];
        timestamps[JobStatus.CREATED.ordinal()] = 1000L;
        timestamps[JobStatus.SCHEDULED.ordinal()] = 2000L;
        timestamps[JobStatus.RUNNING.ordinal()] = 3000L;
        return timestamps;
    }

    private SubPlan mockSubPlan(int pipelineId, PipelineStatus status, int restoreNum, int maxNum) {
        SubPlan subPlan = mock(SubPlan.class);
        when(subPlan.getPipelineId()).thenReturn(pipelineId);
        when(subPlan.getPipelineState()).thenReturn(status);
        when(subPlan.getPipelineRestoreNum()).thenReturn(restoreNum);
        when(subPlan.getPipelineMaxRestoreNum()).thenReturn(maxNum);
        when(subPlan.getPipelineLocation()).thenReturn(new PipelineLocation(JOB_ID, pipelineId));
        return subPlan;
    }

    private void mockPipelines(SubPlan... subPlans) {
        JobMaster jobMaster = mock(JobMaster.class);
        PhysicalPlan physicalPlan = mock(PhysicalPlan.class);
        when(coordinatorService.getJobMaster(JOB_ID)).thenReturn(jobMaster);
        when(jobMaster.getPhysicalPlan()).thenReturn(physicalPlan);
        when(physicalPlan.getPipelineList()).thenReturn(Arrays.asList(subPlans));
    }

    @Test
    void testJobAndPipelineSignalsAreExposed() {
        when(stateTimestampsMap.get(JOB_ID)).thenReturn(jobTimestamps());
        Long[] pipelineTimestamps = new Long[PipelineStatus.values().length];
        pipelineTimestamps[PipelineStatus.RUNNING.ordinal()] = 3500L;
        when(stateTimestampsMap.get(new PipelineLocation(JOB_ID, 1)))
                .thenReturn(pipelineTimestamps);
        mockPipelines(
                mockSubPlan(1, PipelineStatus.RUNNING, 7, 100),
                mockSubPlan(2, PipelineStatus.RUNNING, 2, 100));

        JsonObject diagnostics = JobRuntimeDiagnostics.build(server, JOB_ID);

        Assertions.assertEquals(
                String.valueOf(JOB_ID), diagnostics.getString(JobRuntimeDiagnostics.JOB_ID, null));
        JsonObject jobStateTimestamps =
                diagnostics.get(JobRuntimeDiagnostics.STATE_TIMESTAMPS).asObject();
        Assertions.assertEquals(1000L, jobStateTimestamps.getLong(JobStatus.CREATED.name(), -1L));
        Assertions.assertEquals(3000L, jobStateTimestamps.getLong(JobStatus.RUNNING.name(), -1L));
        // states never entered must not be rendered at all
        Assertions.assertNull(jobStateTimestamps.get(JobStatus.FAILED.name()));

        JsonArray pipelines = diagnostics.get(JobRuntimeDiagnostics.PIPELINES).asArray();
        Assertions.assertEquals(2, pipelines.size());
        JsonObject firstPipeline = pipelines.get(0).asObject();
        Assertions.assertEquals(1, firstPipeline.getInt(JobRuntimeDiagnostics.PIPELINE_ID, -1));
        Assertions.assertEquals(
                PipelineStatus.RUNNING.name(),
                firstPipeline.getString(JobRuntimeDiagnostics.PIPELINE_STATUS, null));
        Assertions.assertEquals(7, firstPipeline.getInt(JobRuntimeDiagnostics.RESTORE_COUNT, -1));
        Assertions.assertEquals(
                100, firstPipeline.getInt(JobRuntimeDiagnostics.MAX_RESTORE_COUNT, -1));
        Assertions.assertEquals(
                3500L,
                firstPipeline
                        .get(JobRuntimeDiagnostics.STATE_TIMESTAMPS)
                        .asObject()
                        .getLong(PipelineStatus.RUNNING.name(), -1L));
        // the second pipeline has no timestamps entry at all, which must not fail the payload
        Assertions.assertTrue(
                pipelines
                        .get(1)
                        .asObject()
                        .get(JobRuntimeDiagnostics.STATE_TIMESTAMPS)
                        .asObject()
                        .isEmpty());
        Assertions.assertEquals(
                9, diagnostics.getInt(JobRuntimeDiagnostics.TOTAL_PIPELINE_RESTORE_COUNT, -1));
    }

    @Test
    void testJobWithoutJobMasterStillReportsStateTimestamps() {
        when(stateTimestampsMap.get(JOB_ID)).thenReturn(jobTimestamps());
        when(coordinatorService.getJobMaster(JOB_ID)).thenReturn(null);

        JsonObject diagnostics = JobRuntimeDiagnostics.build(server, JOB_ID);

        Assertions.assertEquals(
                2000L,
                diagnostics
                        .get(JobRuntimeDiagnostics.STATE_TIMESTAMPS)
                        .asObject()
                        .getLong(JobStatus.SCHEDULED.name(), -1L));
        Assertions.assertTrue(diagnostics.get(JobRuntimeDiagnostics.PIPELINES).asArray().isEmpty());
        Assertions.assertEquals(
                0, diagnostics.getInt(JobRuntimeDiagnostics.TOTAL_PIPELINE_RESTORE_COUNT, -1));
    }

    @Test
    void testPipelineLookupFailureIsIsolated() {
        when(stateTimestampsMap.get(JOB_ID)).thenReturn(jobTimestamps());
        doThrow(new RuntimeException("not the master node"))
                .when(coordinatorService)
                .getJobMaster(JOB_ID);

        JsonObject diagnostics = JobRuntimeDiagnostics.build(server, JOB_ID);

        Assertions.assertTrue(diagnostics.get(JobRuntimeDiagnostics.PIPELINES).asArray().isEmpty());
        Assertions.assertFalse(
                diagnostics.get(JobRuntimeDiagnostics.STATE_TIMESTAMPS).asObject().isEmpty());
    }

    @Test
    void testCleanedStateTimestampsRenderAsEmptyObject() {
        when(stateTimestampsMap.get(JOB_ID)).thenReturn(null);
        when(coordinatorService.getJobMaster(JOB_ID)).thenReturn(null);

        JsonObject diagnostics = JobRuntimeDiagnostics.build(server, JOB_ID);

        Assertions.assertTrue(
                diagnostics.get(JobRuntimeDiagnostics.STATE_TIMESTAMPS).asObject().isEmpty());
    }

    @Test
    void testShorterTimestampsArrayIsToleratedForRollingUpgrade() {
        // an older member may have written an array shorter than the current enum
        when(stateTimestampsMap.get(JOB_ID)).thenReturn(new Long[] {1000L});
        mockPipelines(mockSubPlan(1, null, 0, 3));
        when(stateTimestampsMap.get(new PipelineLocation(JOB_ID, 1))).thenReturn(new Long[] {null});

        JsonObject diagnostics = JobRuntimeDiagnostics.build(server, JOB_ID);

        Assertions.assertEquals(
                1000L,
                diagnostics
                        .get(JobRuntimeDiagnostics.STATE_TIMESTAMPS)
                        .asObject()
                        .getLong(JobStatus.values()[0].name(), -1L));
        JsonObject pipeline =
                diagnostics.get(JobRuntimeDiagnostics.PIPELINES).asArray().get(0).asObject();
        Assertions.assertTrue(pipeline.get(JobRuntimeDiagnostics.PIPELINE_STATUS).isNull());
        Assertions.assertEquals(
                Collections.emptyList(),
                pipeline.get(JobRuntimeDiagnostics.STATE_TIMESTAMPS).asObject().names());
    }
}
