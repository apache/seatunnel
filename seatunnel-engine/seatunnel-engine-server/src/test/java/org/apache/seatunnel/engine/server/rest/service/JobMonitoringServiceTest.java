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

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.master.JobHistoryService;
import org.apache.seatunnel.engine.server.master.JobHistoryService.JobState;
import org.apache.seatunnel.engine.server.master.JobMonitoringRecord;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.collection.IQueue;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

/** Verifies the insertion-sequence contract and bounded reads of the monitoring endpoint. */
class JobMonitoringServiceTest extends AbstractSeaTunnelServerTest {

    private IMap<Long, JobMonitoringRecord> monitoringRecordMap;

    private IMap<String, Long> monitoringMetadataMap;

    private IQueue<JobMonitoringRecord> pendingMonitoringRecordQueue;

    private IMap<Long, JobMonitoringRecord> overflowMonitoringRecordMap;

    private JobHistoryService jobHistoryService;

    private JobMonitoringService jobMonitoringService;

    @BeforeEach
    void setUpMonitoringService() {
        monitoringRecordMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_FINISHED_JOB_MONITORING);
        monitoringMetadataMap =
                nodeEngine
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_FINISHED_JOB_MONITORING_METADATA);
        pendingMonitoringRecordQueue =
                nodeEngine
                        .getHazelcastInstance()
                        .getQueue(Constant.IMAP_FINISHED_JOB_MONITORING_PENDING);
        overflowMonitoringRecordMap =
                nodeEngine
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_FINISHED_JOB_MONITORING_OVERFLOW);
        monitoringRecordMap.clear();
        monitoringMetadataMap.clear();
        pendingMonitoringRecordQueue.clear();
        overflowMonitoringRecordMap.clear();
        nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_FINISHED_JOB_STATE).clear();
        jobHistoryService = server.getCoordinatorService().getJobHistoryService();
        jobMonitoringService = new JobMonitoringService((NodeEngineImpl) nodeEngine);
    }

    @AfterEach
    void clearMonitoringRecords() {
        monitoringRecordMap.clear();
        monitoringMetadataMap.clear();
        pendingMonitoringRecordQueue.clear();
        overflowMonitoringRecordMap.clear();
    }

    @Test
    void testLateOlderCompletionIsNotMissed() {
        jobHistoryService.storeFinishedJobState(
                jobState(20L, JobStatus.FAILED, 2000L, "failure-20"));
        awaitMonitoringSequence(1L);

        JsonObject firstPage =
                jobMonitoringService.getFinishedJobChanges("FAILED", "beginning", null, "1");
        String cursor = firstPage.getString("nextCursor", null);
        Assertions.assertEquals(
                "20", firstPage.get("data").asArray().get(0).asObject().getString("jobId", null));

        jobHistoryService.storeFinishedJobState(
                jobState(-10L, JobStatus.FAILED, 1000L, "late failure"));
        awaitMonitoringSequence(2L);
        JsonObject secondPage = jobMonitoringService.getFinishedJobChanges(null, null, cursor, "1");

        Assertions.assertEquals(
                "-10", secondPage.get("data").asArray().get(0).asObject().getString("jobId", null));
        Assertions.assertEquals(
                2L, secondPage.get("data").asArray().get(0).asObject().getLong("sequence", 0));
    }

    @Test
    void testSequenceWindowIsBoundedAndSkipsExpiredGaps() {
        monitoringRecordMap.put(1L, monitoringRecord(1L, 1L, JobStatus.FAILED, 1000L, "failure-1"));
        monitoringRecordMap.put(
                10000L, monitoringRecord(10000L, 2L, JobStatus.FAILED, 2000L, "failure-2"));
        monitoringMetadataMap.put(Constant.FINISHED_JOB_MONITORING_COMMITTED_SEQUENCE_KEY, 10000L);
        monitoringMetadataMap.put(Constant.FINISHED_JOB_MONITORING_HEAD_SEQUENCE_KEY, 10000L);

        JsonObject firstPage =
                jobMonitoringService.getFinishedJobChanges(null, "beginning", null, "10");
        Assertions.assertEquals(1, firstPage.getInt("scanned", 0));
        Assertions.assertEquals(1, firstPage.get("data").asArray().size());
        Assertions.assertFalse(firstPage.getBoolean("hasMore", true));
        Assertions.assertEquals(10000L, firstPage.getLong("headSequence", 0L));
    }

    @Test
    void testLatestStartAndStatusCursorContract() {
        jobHistoryService.storeFinishedJobState(jobState(1L, JobStatus.FINISHED, 1000L, null));
        awaitMonitoringSequence(1L);

        JsonObject initial =
                jobMonitoringService.getFinishedJobChanges("FAILED", "latest", null, null);
        Assertions.assertEquals(0, initial.get("data").asArray().size());
        Assertions.assertFalse(initial.getBoolean("hasMore", true));

        jobHistoryService.storeFinishedJobState(jobState(2L, JobStatus.FAILED, 900L, "failure-2"));
        awaitMonitoringSequence(2L);
        JsonObject changes =
                jobMonitoringService.getFinishedJobChanges(
                        null, null, initial.getString("nextCursor", null), null);
        Assertions.assertEquals(1, changes.get("data").asArray().size());
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        jobMonitoringService.getFinishedJobChanges(
                                "FINISHED", null, changes.getString("nextCursor", null), null));
    }

    @Test
    void testNullFinishTimeAndTextBounds() {
        String longError = String.join("", Collections.nCopies(2000, "x"));
        jobHistoryService.storeFinishedJobState(
                new JobState(
                        1L,
                        String.join("", Collections.nCopies(400, "n")),
                        JobStatus.FAILED,
                        1000L,
                        null,
                        null,
                        Collections.emptyMap(),
                        longError));
        awaitMonitoringSequence(1L);

        JsonObject item =
                jobMonitoringService
                        .getFinishedJobChanges(null, "beginning", null, "1")
                        .get("data")
                        .asArray()
                        .get(0)
                        .asObject();
        Assertions.assertTrue(item.get("finishTime").isNull());
        Assertions.assertEquals(256, item.getString("jobName", null).length());
        Assertions.assertEquals(1024, item.getString("errorSummary", null).length());
        Assertions.assertTrue(item.getLong("observedTime", 0L) > 0);
    }

    @Test
    void testInvalidParameters() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> jobMonitoringService.getFinishedJobChanges(null, null, null, null));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        jobMonitoringService.getFinishedJobChanges(
                                null, "beginning", "invalid", null));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> jobMonitoringService.getFinishedJobChanges("RUNNING", "latest", null, null));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> jobMonitoringService.getFinishedJobChanges(null, "latest", null, "0"));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> jobMonitoringService.getFinishedJobChanges(null, null, "invalid", null));
    }

    @Test
    void testStaleCursorIsAdvancedToRetentionHead() {
        monitoringRecordMap.put(1L, monitoringRecord(1L, 1L, JobStatus.FAILED, 1000L, "failure-1"));
        monitoringMetadataMap.put(Constant.FINISHED_JOB_MONITORING_COMMITTED_SEQUENCE_KEY, 1L);
        monitoringMetadataMap.put(Constant.FINISHED_JOB_MONITORING_HEAD_SEQUENCE_KEY, 1L);
        String staleCursor =
                jobMonitoringService
                        .getFinishedJobChanges(null, "beginning", null, "1")
                        .getString("nextCursor", null);

        monitoringRecordMap.clear();
        monitoringRecordMap.put(
                10000L, monitoringRecord(10000L, 2L, JobStatus.FAILED, 2000L, "failure-2"));
        monitoringMetadataMap.put(Constant.FINISHED_JOB_MONITORING_COMMITTED_SEQUENCE_KEY, 10000L);
        monitoringMetadataMap.put(Constant.FINISHED_JOB_MONITORING_HEAD_SEQUENCE_KEY, 10000L);

        JsonObject response =
                jobMonitoringService.getFinishedJobChanges(null, null, staleCursor, "10");
        Assertions.assertTrue(response.getBoolean("cursorReset", false));
        Assertions.assertEquals(1, response.getInt("scanned", 0));
        Assertions.assertEquals(
                "2", response.get("data").asArray().get(0).asObject().getString("jobId", null));
    }

    @Test
    void testUncommittedRecordIsReconciledBeforeNextAppend() {
        monitoringRecordMap.put(1L, monitoringRecord(1L, 1L, JobStatus.FAILED, 1000L, "failure-1"));

        jobHistoryService.storeFinishedJobState(jobState(2L, JobStatus.FAILED, 900L, "failure-2"));
        awaitMonitoringSequence(2L);

        JsonObject response =
                jobMonitoringService.getFinishedJobChanges(null, "beginning", null, "2");
        Assertions.assertEquals(2, response.get("data").asArray().size());
        Assertions.assertEquals(
                "1", response.get("data").asArray().get(0).asObject().getString("jobId", null));
        Assertions.assertEquals(
                "2", response.get("data").asArray().get(1).asObject().getString("jobId", null));
    }

    private void awaitMonitoringSequence(long expectedSequence) {
        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        expectedSequence,
                                        monitoringMetadataMap.getOrDefault(
                                                Constant
                                                        .FINISHED_JOB_MONITORING_COMMITTED_SEQUENCE_KEY,
                                                0L)));
    }

    private JobState jobState(long jobId, JobStatus status, Long finishTime, String errorMessage) {
        return new JobState(
                jobId,
                "job-" + jobId,
                status,
                500L,
                null,
                finishTime,
                Collections.emptyMap(),
                errorMessage);
    }

    private JobMonitoringRecord monitoringRecord(
            long sequence, long jobId, JobStatus status, Long finishTime, String errorSummary) {
        return new JobMonitoringRecord(
                sequence,
                jobId,
                "job-" + jobId,
                status,
                500L,
                null,
                finishTime,
                3000L,
                errorSummary);
    }
}
