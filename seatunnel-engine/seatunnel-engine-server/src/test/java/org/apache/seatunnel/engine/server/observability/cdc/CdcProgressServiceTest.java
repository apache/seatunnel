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

package org.apache.seatunnel.engine.server.observability.cdc;

import org.apache.seatunnel.api.cdc.CdcEnumeratorProgressReport;
import org.apache.seatunnel.api.cdc.CdcProgressLifecycle;
import org.apache.seatunnel.api.cdc.CdcProgressValue;
import org.apache.seatunnel.api.cdc.CdcReaderProgressReport;
import org.apache.seatunnel.api.cdc.CdcSnapshotAssignmentStatus;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;

class CdcProgressServiceTest {

    @Test
    void testEnvelopeRejectsMismatchedOwnerAndPayload() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        new CdcProgressEnvelope<>(
                                CdcProgressOwner.ENUMERATOR,
                                taskLocation(1L, 2, 0),
                                10L,
                                100L,
                                1L,
                                1_000L,
                                readerReport("split")));
    }

    @Test
    void testRejectsStaleSequenceAndPreviousExecutionAttempt() {
        CdcProgressService service = new CdcProgressService();
        TaskLocation taskLocation = taskLocation(1L, 2, 0);

        service.updateReports(
                Arrays.asList(
                        readerEnvelope(taskLocation, 10L, 100L, 2L, "newer-sequence"),
                        readerEnvelope(taskLocation, 10L, 100L, 1L, "stale-sequence")));
        service.updateReports(
                Collections.singletonList(
                        readerEnvelope(taskLocation, 10L, 101L, 1L, "new-attempt")));
        service.updateReports(
                Collections.singletonList(
                        readerEnvelope(taskLocation, 10L, 100L, 3L, "old-attempt")));

        CdcProgressEnvelope<CdcReaderProgressReport> stored =
                service.getReaderReports(1L, 2, 10L).get(0);
        Assertions.assertEquals(101L, stored.getExecutionAttemptId());
        Assertions.assertEquals("new-attempt", stored.getReport().getActiveSplitId());
    }

    @Test
    void testKeepsSourceVerticesAndReaderIndexesSeparate() {
        CdcProgressService service = new CdcProgressService();

        service.updateReports(
                Arrays.asList(
                        readerEnvelope(taskLocation(1L, 2, 0), 10L, 100L, 1L, "source-10-0"),
                        readerEnvelope(taskLocation(1L, 2, 1), 10L, 100L, 1L, "source-10-1"),
                        readerEnvelope(taskLocation(1L, 2, 0), 11L, 100L, 1L, "source-11-0")));

        Assertions.assertEquals(2, service.getReaderReports(1L, 2, 10L).size());
        Assertions.assertEquals(1, service.getReaderReports(1L, 2, 11L).size());
    }

    @Test
    void testRejectsStaleEnumeratorReports() {
        CdcProgressService service = new CdcProgressService();
        TaskLocation taskLocation = taskLocation(1L, 2, 0);

        service.updateReports(
                Arrays.asList(
                        enumeratorEnvelope(taskLocation, 10L, 100L, 2L, 1_000L),
                        enumeratorEnvelope(taskLocation, 10L, 100L, 1L, 2_000L)));
        service.updateReports(
                Collections.singletonList(enumeratorEnvelope(taskLocation, 10L, 101L, 1L, 1_000L)));
        service.updateReports(
                Collections.singletonList(enumeratorEnvelope(taskLocation, 10L, 100L, 3L, 3_000L)));

        CdcProgressEnvelope<CdcEnumeratorProgressReport> stored =
                service.getEnumeratorReport(1L, 2, 10L);
        Assertions.assertEquals(101L, stored.getExecutionAttemptId());
        Assertions.assertEquals(1L, stored.getReportSequence());
        Assertions.assertEquals(1_000L, stored.getObservedAt());
    }

    @Test
    void testPipelineCleanupRemovesOnlyMatchingReports() {
        CdcProgressService service = new CdcProgressService();
        service.updateReports(
                Arrays.asList(
                        readerEnvelope(taskLocation(1L, 2, 0), 10L, 100L, 1L, "removed"),
                        readerEnvelope(taskLocation(1L, 3, 0), 10L, 100L, 1L, "retained")));
        service.updateReports(
                Arrays.asList(
                        enumeratorEnvelope(taskLocation(1L, 2, 0), 10L, 100L, 1L, 1_000L),
                        enumeratorEnvelope(taskLocation(1L, 3, 0), 10L, 100L, 1L, 1_000L)));

        service.removePipeline(new PipelineLocation(1L, 2));

        Assertions.assertTrue(service.getReaderReports(1L, 2, 10L).isEmpty());
        Assertions.assertEquals(1, service.getReaderReports(1L, 3, 10L).size());
        Assertions.assertNull(service.getEnumeratorReport(1L, 2, 10L));
        Assertions.assertNotNull(service.getEnumeratorReport(1L, 3, 10L));
    }

    private CdcProgressEnvelope<CdcReaderProgressReport> readerEnvelope(
            TaskLocation taskLocation,
            long sourceVertexId,
            long executionAttemptId,
            long sequence,
            String splitId) {
        CdcReaderProgressReport report = readerReport(splitId);
        return new CdcProgressEnvelope<>(
                CdcProgressOwner.READER,
                taskLocation,
                sourceVertexId,
                executionAttemptId,
                sequence,
                1000L,
                report);
    }

    private CdcReaderProgressReport readerReport(String splitId) {
        return new CdcReaderProgressReport(
                "MySQL-CDC",
                CdcProgressLifecycle.INCREMENTAL,
                splitId,
                CdcProgressValue.unavailable(),
                CdcProgressValue.unsupported(),
                CdcProgressValue.unsupported(),
                0L,
                null);
    }

    private CdcProgressEnvelope<CdcEnumeratorProgressReport> enumeratorEnvelope(
            TaskLocation taskLocation,
            long sourceVertexId,
            long executionAttemptId,
            long sequence,
            long observedAt) {
        CdcEnumeratorProgressReport report =
                new CdcEnumeratorProgressReport(
                        "MySQL-CDC",
                        CdcSnapshotAssignmentStatus.ASSIGNING,
                        CdcProgressValue.exact(0),
                        CdcProgressValue.exact(0),
                        CdcProgressValue.exact(0),
                        CdcProgressValue.exact(0),
                        CdcProgressValue.exact(0),
                        Collections.emptyList());
        return new CdcProgressEnvelope<>(
                CdcProgressOwner.ENUMERATOR,
                taskLocation,
                sourceVertexId,
                executionAttemptId,
                sequence,
                observedAt,
                report);
    }

    private TaskLocation taskLocation(long jobId, int pipelineId, int taskIndex) {
        return new TaskLocation(new TaskGroupLocation(jobId, pipelineId, 1L), 1L, taskIndex);
    }
}
