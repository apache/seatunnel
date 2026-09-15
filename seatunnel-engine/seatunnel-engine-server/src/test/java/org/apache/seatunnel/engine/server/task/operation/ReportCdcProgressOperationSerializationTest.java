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

package org.apache.seatunnel.engine.server.task.operation;

import org.apache.seatunnel.api.cdc.CdcEnumeratorProgressReport;
import org.apache.seatunnel.api.cdc.CdcProgressAccuracy;
import org.apache.seatunnel.api.cdc.CdcProgressLifecycle;
import org.apache.seatunnel.api.cdc.CdcProgressPosition;
import org.apache.seatunnel.api.cdc.CdcProgressValue;
import org.apache.seatunnel.api.cdc.CdcReaderProgressReport;
import org.apache.seatunnel.api.cdc.CdcSnapshotAssignmentStatus;
import org.apache.seatunnel.api.cdc.CdcSnapshotSplitProgress;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.observability.cdc.CdcProgressEnvelope;
import org.apache.seatunnel.engine.server.observability.cdc.CdcProgressOwner;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

class ReportCdcProgressOperationSerializationTest {

    private final InternalSerializationService serializationService =
            (InternalSerializationService) new DefaultSerializationServiceBuilder().build();

    @AfterEach
    void tearDown() {
        serializationService.dispose();
    }

    @Test
    void testReportsArePreservedAfterSerialization() {
        TaskLocation location = new TaskLocation(new TaskGroupLocation(1L, 2, 3L), 4L, 0);
        CdcProgressPosition position =
                new CdcProgressPosition(
                        "MYSQL_BINLOG", 1, Collections.singletonMap("file", "mysql-bin.000001"));
        CdcProgressEnvelope<CdcReaderProgressReport> reader =
                new CdcProgressEnvelope<>(
                        CdcProgressOwner.READER,
                        location,
                        5L,
                        6L,
                        7L,
                        8L,
                        new CdcReaderProgressReport(
                                "MySQL-CDC",
                                CdcProgressLifecycle.INCREMENTAL,
                                "incremental-split",
                                CdcProgressValue.exact(position),
                                CdcProgressValue.bestEffort(position),
                                CdcProgressValue.unsupported(),
                                9L,
                                10L));
        CdcProgressEnvelope<CdcEnumeratorProgressReport> enumerator =
                new CdcProgressEnvelope<>(
                        CdcProgressOwner.ENUMERATOR,
                        location,
                        5L,
                        6L,
                        7L,
                        8L,
                        new CdcEnumeratorProgressReport(
                                "MySQL-CDC",
                                CdcSnapshotAssignmentStatus.ASSIGNING,
                                CdcProgressValue.exact(1),
                                CdcProgressValue.exact(0),
                                CdcProgressValue.exact(1),
                                CdcProgressValue.exact(0),
                                CdcProgressValue.exact(0),
                                Collections.singletonList(
                                        new CdcSnapshotSplitProgress(
                                                "snapshot-split-1",
                                                "inventory.orders",
                                                CdcProgressValue.exact(position),
                                                CdcProgressValue.unavailable())),
                                true));
        ReportCdcProgressOperation original =
                new ReportCdcProgressOperation(java.util.Arrays.asList(reader, enumerator));

        Data data = serializationService.toData(original);
        ReportCdcProgressOperation restored = serializationService.toObject(data);

        List<?> reports = reports(restored);
        Assertions.assertEquals(2, reports.size());
        CdcProgressEnvelope<?> restoredReader = (CdcProgressEnvelope<?>) reports.get(0);
        Assertions.assertEquals(CdcProgressOwner.READER, restoredReader.getOwner());
        Assertions.assertEquals(7L, restoredReader.getReportSequence());
        CdcReaderProgressReport restoredReaderReport =
                (CdcReaderProgressReport) restoredReader.getReport();
        Assertions.assertEquals(
                "mysql-bin.000001",
                restoredReaderReport
                        .getCurrentConsumedPosition()
                        .getValue()
                        .getValues()
                        .get("file"));
        Assertions.assertEquals(
                CdcProgressAccuracy.BEST_EFFORT,
                restoredReaderReport.getLastCompletedCheckpointPosition().getAccuracy());
        Assertions.assertEquals(10L, restoredReaderReport.getLastSourceEventAt());
        CdcProgressEnvelope<?> restoredEnumerator = (CdcProgressEnvelope<?>) reports.get(1);
        Assertions.assertEquals(CdcProgressOwner.ENUMERATOR, restoredEnumerator.getOwner());
        Assertions.assertEquals(5L, restoredEnumerator.getSourceVertexId());
        Assertions.assertEquals(7L, restoredEnumerator.getReportSequence());
        Assertions.assertEquals(
                "snapshot-split-1",
                ((CdcEnumeratorProgressReport) restoredEnumerator.getReport())
                        .getActiveSplits()
                        .get(0)
                        .getSplitId());
        Assertions.assertEquals(
                "MYSQL_BINLOG",
                ((CdcEnumeratorProgressReport) restoredEnumerator.getReport())
                        .getActiveSplits()
                        .get(0)
                        .getLowWatermark()
                        .getValue()
                        .getType());
        Assertions.assertTrue(
                ((CdcEnumeratorProgressReport) restoredEnumerator.getReport())
                        .isActiveSplitsTruncated());

        CdcProgressReportBatch batch =
                new CdcProgressReportBatch(java.util.Arrays.asList(reader, enumerator));
        CdcProgressReportBatch restoredBatch =
                serializationService.toObject(serializationService.toData(batch));
        Assertions.assertEquals(2, restoredBatch.getReports().size());
        Assertions.assertEquals(
                CdcProgressOwner.ENUMERATOR, restoredBatch.getReports().get(1).getOwner());
    }

    @Test
    void testEveryProgressOwnerRoundTripsByName() {
        for (CdcProgressOwner owner : CdcProgressOwner.values()) {
            CdcProgressEnvelope<?> restored =
                    roundTrip(
                            owner == CdcProgressOwner.READER
                                    ? readerEnvelope(
                                            CdcProgressLifecycle.INCREMENTAL,
                                            CdcProgressValue.unavailable())
                                    : enumeratorEnvelope(CdcSnapshotAssignmentStatus.ASSIGNING));

            Assertions.assertEquals(owner, restored.getOwner());
        }
    }

    @Test
    void testEveryProgressLifecycleRoundTripsByName() {
        for (CdcProgressLifecycle lifecycle : CdcProgressLifecycle.values()) {
            CdcReaderProgressReport restored =
                    (CdcReaderProgressReport)
                            roundTrip(readerEnvelope(lifecycle, CdcProgressValue.unavailable()))
                                    .getReport();

            Assertions.assertEquals(lifecycle, restored.getLifecycle());
        }
    }

    @Test
    void testEveryProgressAccuracyRoundTripsByName() {
        for (CdcProgressAccuracy accuracy : CdcProgressAccuracy.values()) {
            CdcReaderProgressReport restored =
                    (CdcReaderProgressReport)
                            roundTrip(
                                            readerEnvelope(
                                                    CdcProgressLifecycle.INCREMENTAL,
                                                    value(accuracy)))
                                    .getReport();

            Assertions.assertEquals(accuracy, restored.getCurrentConsumedPosition().getAccuracy());
        }
    }

    @Test
    void testEverySnapshotAssignmentStatusRoundTripsByName() {
        for (CdcSnapshotAssignmentStatus status : CdcSnapshotAssignmentStatus.values()) {
            CdcEnumeratorProgressReport restored =
                    (CdcEnumeratorProgressReport) roundTrip(enumeratorEnvelope(status)).getReport();

            Assertions.assertEquals(status, restored.getSnapshotAssignmentStatus());
        }
    }

    @Test
    void testEmptyReportListsArePreservedAfterSerialization() {
        ReportCdcProgressOperation original =
                new ReportCdcProgressOperation(Collections.emptyList());

        Data data = serializationService.toData(original);
        ReportCdcProgressOperation restored = serializationService.toObject(data);

        Assertions.assertTrue(reports(restored).isEmpty());
    }

    @Test
    void testEmptyReportBatchIsPreservedAfterSerialization() {
        CdcProgressReportBatch original = new CdcProgressReportBatch(Collections.emptyList());

        Data data = serializationService.toData(original);
        CdcProgressReportBatch restored = serializationService.toObject(data);

        Assertions.assertTrue(restored.getReports().isEmpty());
    }

    @Test
    void testEnumeratorCollectionRequestIsPreservedAfterSerialization() {
        TaskGroupLocation location = new TaskGroupLocation(1L, 2, 3L);
        CollectCdcEnumeratorProgressOperation original =
                new CollectCdcEnumeratorProgressOperation(Collections.singletonList(location));

        Data data = serializationService.toData(original);
        CollectCdcEnumeratorProgressOperation restored = serializationService.toObject(data);

        List<?> taskGroupLocations =
                ReflectionUtils.getField(restored, "taskGroupLocations")
                        .map(field -> (List<?>) field)
                        .orElseThrow(() -> new AssertionError("Missing taskGroupLocations field"));
        Assertions.assertEquals(Collections.singletonList(location), taskGroupLocations);
    }

    @Test
    void testRejectsNegativeCollectionSize() throws IOException {
        BufferObjectDataOutput output = serializationService.createObjectDataOutput();
        output.writeInt(-1);
        BufferObjectDataInput input =
                serializationService.createObjectDataInput(output.toByteArray());

        IOException exception =
                Assertions.assertThrows(
                        IOException.class,
                        () -> CdcProgressReportSerializer.readSize(input, "report"));

        Assertions.assertEquals("Invalid CDC progress report count: -1", exception.getMessage());
    }

    @Test
    void testRejectsUnknownReportOwner() throws IOException {
        BufferObjectDataOutput output = serializationService.createObjectDataOutput();
        output.writeString("UNKNOWN");
        BufferObjectDataInput input =
                serializationService.createObjectDataInput(output.toByteArray());

        IOException exception =
                Assertions.assertThrows(
                        IOException.class, () -> CdcProgressReportSerializer.readEnvelope(input));

        Assertions.assertEquals("Unknown CDC progress owner: UNKNOWN", exception.getMessage());
    }

    private CdcProgressEnvelope<CdcReaderProgressReport> readerEnvelope(
            CdcProgressLifecycle lifecycle, CdcProgressValue<CdcProgressPosition> currentPosition) {
        return new CdcProgressEnvelope<>(
                CdcProgressOwner.READER,
                taskLocation(),
                5L,
                6L,
                7L,
                8L,
                new CdcReaderProgressReport(
                        "MySQL-CDC",
                        lifecycle,
                        "incremental-split",
                        currentPosition,
                        CdcProgressValue.unavailable(),
                        CdcProgressValue.unavailable(),
                        9L,
                        null));
    }

    private CdcProgressEnvelope<CdcEnumeratorProgressReport> enumeratorEnvelope(
            CdcSnapshotAssignmentStatus status) {
        return new CdcProgressEnvelope<>(
                CdcProgressOwner.ENUMERATOR,
                taskLocation(),
                5L,
                6L,
                7L,
                8L,
                new CdcEnumeratorProgressReport(
                        "MySQL-CDC",
                        status,
                        CdcProgressValue.exact(0),
                        CdcProgressValue.exact(0),
                        CdcProgressValue.exact(0),
                        CdcProgressValue.exact(0),
                        CdcProgressValue.exact(0),
                        Collections.emptyList()));
    }

    private CdcProgressValue<CdcProgressPosition> value(CdcProgressAccuracy accuracy) {
        CdcProgressPosition position =
                new CdcProgressPosition(
                        "MYSQL_BINLOG", 1, Collections.singletonMap("file", "mysql-bin.000001"));
        switch (accuracy) {
            case EXACT:
                return CdcProgressValue.exact(position);
            case BEST_EFFORT:
                return CdcProgressValue.bestEffort(position);
            case UNSUPPORTED:
                return CdcProgressValue.unsupported();
            case UNAVAILABLE:
                return CdcProgressValue.unavailable();
            default:
                throw new AssertionError("Unhandled accuracy: " + accuracy);
        }
    }

    private CdcProgressEnvelope<?> roundTrip(CdcProgressEnvelope<?> envelope) {
        ReportCdcProgressOperation operation =
                new ReportCdcProgressOperation(Collections.singletonList(envelope));
        ReportCdcProgressOperation restored =
                serializationService.toObject(serializationService.toData(operation));
        return (CdcProgressEnvelope<?>) reports(restored).get(0);
    }

    private TaskLocation taskLocation() {
        return new TaskLocation(new TaskGroupLocation(1L, 2, 3L), 4L, 0);
    }

    @SuppressWarnings("unchecked")
    private List<?> reports(ReportCdcProgressOperation operation) {
        return ReflectionUtils.getField(operation, "reports")
                .map(field -> (List<Object>) field)
                .orElseThrow(() -> new AssertionError("Missing reports field"));
    }
}
