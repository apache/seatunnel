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
import org.apache.seatunnel.api.cdc.CdcSnapshotSplitProgress;
import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.observability.cdc.CdcEnumeratorProgressEnvelope;
import org.apache.seatunnel.engine.server.observability.cdc.CdcReaderProgressEnvelope;

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
        CdcReaderProgressEnvelope reader =
                new CdcReaderProgressEnvelope(
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
        CdcEnumeratorProgressEnvelope enumerator =
                new CdcEnumeratorProgressEnvelope(
                        location,
                        5L,
                        6L,
                        7L,
                        8L,
                        new CdcEnumeratorProgressReport(
                                "MySQL-CDC",
                                CdcProgressLifecycle.SNAPSHOT,
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
                                                CdcProgressValue.unavailable()))));
        ReportCdcProgressOperation original =
                new ReportCdcProgressOperation(
                        Collections.singletonList(reader), Collections.singletonList(enumerator));

        Data data = serializationService.toData(original);
        ReportCdcProgressOperation restored = serializationService.toObject(data);

        List<?> readerReports = reports(restored, "readerReports");
        List<?> enumeratorReports = reports(restored, "enumeratorReports");
        Assertions.assertEquals(1, readerReports.size());
        CdcReaderProgressEnvelope restoredReader = (CdcReaderProgressEnvelope) readerReports.get(0);
        Assertions.assertEquals(7L, restoredReader.getReportSequence());
        Assertions.assertEquals(
                "mysql-bin.000001",
                restoredReader
                        .getReport()
                        .getCurrentConsumedPosition()
                        .getValue()
                        .getValues()
                        .get("file"));
        Assertions.assertEquals(
                CdcProgressAccuracy.BEST_EFFORT,
                restoredReader.getReport().getLastCompletedCheckpointPosition().getAccuracy());
        Assertions.assertEquals(10L, restoredReader.getReport().getLastSourceEventAt());
        Assertions.assertEquals(1, enumeratorReports.size());
        CdcEnumeratorProgressEnvelope restoredEnumerator =
                (CdcEnumeratorProgressEnvelope) enumeratorReports.get(0);
        Assertions.assertEquals(5L, restoredEnumerator.getSourceVertexId());
        Assertions.assertEquals(7L, restoredEnumerator.getReportSequence());
        Assertions.assertEquals(
                "snapshot-split-1",
                restoredEnumerator.getReport().getActiveSplits().get(0).getSplitId());
        Assertions.assertEquals(
                "MYSQL_BINLOG",
                restoredEnumerator
                        .getReport()
                        .getActiveSplits()
                        .get(0)
                        .getLowWatermark()
                        .getValue()
                        .getType());
    }

    @Test
    void testEmptyReportListsArePreservedAfterSerialization() {
        ReportCdcProgressOperation original =
                new ReportCdcProgressOperation(Collections.emptyList(), Collections.emptyList());

        Data data = serializationService.toData(original);
        ReportCdcProgressOperation restored = serializationService.toObject(data);

        Assertions.assertTrue(reports(restored, "readerReports").isEmpty());
        Assertions.assertTrue(reports(restored, "enumeratorReports").isEmpty());
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
                        () -> CdcProgressReportSerializer.readSize(input, "reader report"));

        Assertions.assertEquals(
                "Invalid CDC progress reader report count: -1", exception.getMessage());
    }

    @SuppressWarnings("unchecked")
    private List<?> reports(ReportCdcProgressOperation operation, String fieldName) {
        return ReflectionUtils.getField(operation, fieldName)
                .map(field -> (List<Object>) field)
                .orElseThrow(() -> new AssertionError("Missing field " + fieldName));
    }
}
