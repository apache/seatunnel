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

package org.apache.seatunnel.api.cdc;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

class CdcProgressModelTest {

    @Test
    void testPositionDefensivelyCopiesValues() {
        Map<String, String> values = new LinkedHashMap<>();
        values.put("file", "mysql-bin.000001");

        CdcProgressPosition position = new CdcProgressPosition("MYSQL_BINLOG", 1, values);
        values.put("pos", "100");

        Assertions.assertEquals(
                Collections.singletonMap("file", "mysql-bin.000001"), position.getValues());
        Assertions.assertThrows(
                UnsupportedOperationException.class, () -> position.getValues().put("pos", "200"));
    }

    @Test
    void testEnumeratorReportDefensivelyCopiesActiveSplits() {
        List<CdcSnapshotSplitProgress> activeSplits = new ArrayList<>();
        activeSplits.add(
                new CdcSnapshotSplitProgress(
                        "split-1",
                        "inventory.orders",
                        CdcProgressValue.unavailable(),
                        CdcProgressValue.unavailable()));

        CdcEnumeratorProgressReport report =
                new CdcEnumeratorProgressReport(
                        "MySQL-CDC",
                        CdcSnapshotAssignmentStatus.ASSIGNING,
                        CdcProgressValue.exact(1),
                        CdcProgressValue.exact(0),
                        CdcProgressValue.exact(1),
                        CdcProgressValue.exact(0),
                        CdcProgressValue.exact(0),
                        activeSplits);
        activeSplits.clear();

        Assertions.assertEquals(1, report.getActiveSplits().size());
        Assertions.assertThrows(
                UnsupportedOperationException.class, () -> report.getActiveSplits().clear());
    }

    @Test
    void testEnumeratorReportBoundsActiveSplitDetails() {
        List<CdcSnapshotSplitProgress> activeSplits = new ArrayList<>();
        for (int i = 0; i <= CdcEnumeratorProgressReport.MAX_ACTIVE_SPLITS; i++) {
            activeSplits.add(activeSplit("split-" + i));
        }

        CdcEnumeratorProgressReport report =
                new CdcEnumeratorProgressReport(
                        "MySQL-CDC",
                        CdcSnapshotAssignmentStatus.ASSIGNING,
                        CdcProgressValue.exact(activeSplits.size()),
                        CdcProgressValue.exact(0),
                        CdcProgressValue.exact(activeSplits.size()),
                        CdcProgressValue.exact(0),
                        CdcProgressValue.exact(0),
                        activeSplits);

        Assertions.assertEquals(
                CdcEnumeratorProgressReport.MAX_ACTIVE_SPLITS, report.getActiveSplits().size());
        Assertions.assertTrue(report.isActiveSplitsTruncated());
    }

    @Test
    void testEnumeratorReportRejectsInvalidCounts() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        new CdcEnumeratorProgressReport(
                                "MySQL-CDC",
                                CdcSnapshotAssignmentStatus.ASSIGNING,
                                CdcProgressValue.exact(-1),
                                CdcProgressValue.exact(0),
                                CdcProgressValue.exact(-1),
                                CdcProgressValue.exact(0),
                                CdcProgressValue.exact(0),
                                Collections.emptyList()));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        new CdcEnumeratorProgressReport(
                                "MySQL-CDC",
                                CdcSnapshotAssignmentStatus.ASSIGNING,
                                CdcProgressValue.exact(2),
                                CdcProgressValue.exact(0),
                                CdcProgressValue.exact(1),
                                CdcProgressValue.exact(0),
                                CdcProgressValue.exact(0),
                                Collections.singletonList(activeSplit("split-1"))));
    }

    @Test
    void testUnsupportedAndUnavailableValuesCarryNoPayload() {
        CdcProgressValue<Integer> unsupported = CdcProgressValue.unsupported();
        CdcProgressValue<Integer> unavailable = CdcProgressValue.unavailable();

        Assertions.assertNull(unsupported.getValue());
        Assertions.assertEquals(CdcProgressAccuracy.UNSUPPORTED, unsupported.getAccuracy());
        Assertions.assertNull(unavailable.getValue());
        Assertions.assertEquals(CdcProgressAccuracy.UNAVAILABLE, unavailable.getAccuracy());
    }

    @Test
    void testProgressValueDoesNotRequireJavaSerialization() {
        Object value = new Object();

        Assertions.assertSame(value, CdcProgressValue.exact(value).getValue());
    }

    private static CdcSnapshotSplitProgress activeSplit(String splitId) {
        return new CdcSnapshotSplitProgress(
                splitId,
                "inventory.orders",
                CdcProgressValue.unavailable(),
                CdcProgressValue.unavailable());
    }
}
