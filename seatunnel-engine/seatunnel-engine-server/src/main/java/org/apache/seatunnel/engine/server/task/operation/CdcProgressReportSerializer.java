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
import org.apache.seatunnel.api.cdc.CdcProgressReport;
import org.apache.seatunnel.api.cdc.CdcProgressValue;
import org.apache.seatunnel.api.cdc.CdcReaderProgressReport;
import org.apache.seatunnel.api.cdc.CdcSnapshotAssignmentStatus;
import org.apache.seatunnel.api.cdc.CdcSnapshotSplitProgress;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.observability.cdc.CdcProgressEnvelope;
import org.apache.seatunnel.engine.server.observability.cdc.CdcProgressOwner;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/** Serializes CDC progress reports without falling back to Java object deserialization. */
final class CdcProgressReportSerializer {

    private CdcProgressReportSerializer() {}

    static void writeEnvelope(ObjectDataOutput out, CdcProgressEnvelope<?> envelope)
            throws IOException {
        out.writeString(envelope.getOwner().name());
        out.writeObject(envelope.getTaskLocation());
        out.writeLong(envelope.getSourceVertexId());
        out.writeLong(envelope.getExecutionAttemptId());
        out.writeLong(envelope.getReportSequence());
        out.writeLong(envelope.getObservedAt());
        if (envelope.getOwner() == CdcProgressOwner.READER) {
            writeReaderReport(out, (CdcReaderProgressReport) envelope.getReport());
        } else {
            writeEnumeratorReport(out, (CdcEnumeratorProgressReport) envelope.getReport());
        }
    }

    static CdcProgressEnvelope<? extends CdcProgressReport> readEnvelope(ObjectDataInput in)
            throws IOException {
        CdcProgressOwner owner = readOwner(in);
        TaskLocation taskLocation = in.readObject(TaskLocation.class);
        long sourceVertexId = in.readLong();
        long executionAttemptId = in.readLong();
        long reportSequence = in.readLong();
        long observedAt = in.readLong();
        CdcProgressReport report =
                owner == CdcProgressOwner.READER ? readReaderReport(in) : readEnumeratorReport(in);
        return new CdcProgressEnvelope<>(
                owner,
                taskLocation,
                sourceVertexId,
                executionAttemptId,
                reportSequence,
                observedAt,
                report);
    }

    private static void writeReaderReport(ObjectDataOutput out, CdcReaderProgressReport report)
            throws IOException {
        out.writeString(report.getConnectorType());
        writeLifecycle(out, report.getLifecycle());
        out.writeString(report.getActiveSplitId());
        writePositionValue(out, report.getCurrentConsumedPosition());
        writePositionValue(out, report.getLastCompletedCheckpointPosition());
        writePositionValue(out, report.getRestoredPosition());
        out.writeLong(report.getLastPositionChangeAt());
        writeNullableLong(out, report.getLastSourceEventAt());
    }

    private static CdcReaderProgressReport readReaderReport(ObjectDataInput in) throws IOException {
        return new CdcReaderProgressReport(
                in.readString(),
                readLifecycle(in),
                in.readString(),
                readPositionValue(in),
                readPositionValue(in),
                readPositionValue(in),
                in.readLong(),
                readNullableLong(in));
    }

    private static void writeEnumeratorReport(
            ObjectDataOutput out, CdcEnumeratorProgressReport report) throws IOException {
        out.writeString(report.getConnectorType());
        out.writeString(report.getSnapshotAssignmentStatus().name());
        writeIntegerValue(out, report.getAssignedSplitCount());
        writeIntegerValue(out, report.getCompletedSplitCount());
        writeIntegerValue(out, report.getRunningSplitCount());
        writeIntegerValue(out, report.getPreparedRemainingSplitCount());
        writeIntegerValue(out, report.getRemainingUnchunkedTableCount());
        out.writeBoolean(report.isActiveSplitsTruncated());
        out.writeInt(report.getActiveSplits().size());
        for (CdcSnapshotSplitProgress splitProgress : report.getActiveSplits()) {
            out.writeString(splitProgress.getSplitId());
            out.writeString(splitProgress.getTablePath());
            writePositionValue(out, splitProgress.getLowWatermark());
            writePositionValue(out, splitProgress.getHighWatermark());
        }
    }

    private static CdcEnumeratorProgressReport readEnumeratorReport(ObjectDataInput in)
            throws IOException {
        String connectorType = in.readString();
        CdcSnapshotAssignmentStatus assignmentStatus =
                CdcSnapshotAssignmentStatus.valueOf(in.readString());
        CdcProgressValue<Integer> assignedSplitCount = readIntegerValue(in);
        CdcProgressValue<Integer> completedSplitCount = readIntegerValue(in);
        CdcProgressValue<Integer> runningSplitCount = readIntegerValue(in);
        CdcProgressValue<Integer> preparedRemainingSplitCount = readIntegerValue(in);
        CdcProgressValue<Integer> remainingUnchunkedTableCount = readIntegerValue(in);
        boolean activeSplitsTruncated = in.readBoolean();
        int activeSplitCount = readSize(in, "active split");
        List<CdcSnapshotSplitProgress> activeSplits = new ArrayList<>(activeSplitCount);
        for (int i = 0; i < activeSplitCount; i++) {
            activeSplits.add(
                    new CdcSnapshotSplitProgress(
                            in.readString(),
                            in.readString(),
                            readPositionValue(in),
                            readPositionValue(in)));
        }
        return new CdcEnumeratorProgressReport(
                connectorType,
                assignmentStatus,
                assignedSplitCount,
                completedSplitCount,
                runningSplitCount,
                preparedRemainingSplitCount,
                remainingUnchunkedTableCount,
                activeSplits,
                activeSplitsTruncated);
    }

    private static void writePositionValue(
            ObjectDataOutput out, CdcProgressValue<CdcProgressPosition> progressValue)
            throws IOException {
        writeAccuracy(out, progressValue.getAccuracy());
        if (isSupported(progressValue.getAccuracy())) {
            CdcProgressPosition position = progressValue.getValue();
            out.writeString(position.getType());
            out.writeInt(position.getSchemaVersion());
            out.writeInt(position.getValues().size());
            for (Map.Entry<String, String> entry : position.getValues().entrySet()) {
                out.writeString(entry.getKey());
                out.writeString(entry.getValue());
            }
        }
    }

    private static CdcProgressValue<CdcProgressPosition> readPositionValue(ObjectDataInput in)
            throws IOException {
        CdcProgressAccuracy accuracy = readAccuracy(in);
        if (!isSupported(accuracy)) {
            return emptyValue(accuracy);
        }
        String type = in.readString();
        int schemaVersion = in.readInt();
        int valueCount = readSize(in, "position field");
        Map<String, String> values = new LinkedHashMap<>(valueCount);
        for (int i = 0; i < valueCount; i++) {
            values.put(in.readString(), in.readString());
        }
        return supportedValue(accuracy, new CdcProgressPosition(type, schemaVersion, values));
    }

    private static void writeIntegerValue(
            ObjectDataOutput out, CdcProgressValue<Integer> progressValue) throws IOException {
        writeAccuracy(out, progressValue.getAccuracy());
        if (isSupported(progressValue.getAccuracy())) {
            out.writeInt(progressValue.getValue());
        }
    }

    private static CdcProgressValue<Integer> readIntegerValue(ObjectDataInput in)
            throws IOException {
        CdcProgressAccuracy accuracy = readAccuracy(in);
        if (!isSupported(accuracy)) {
            return emptyValue(accuracy);
        }
        return supportedValue(accuracy, in.readInt());
    }

    private static void writeLifecycle(ObjectDataOutput out, CdcProgressLifecycle lifecycle)
            throws IOException {
        out.writeString(lifecycle.name());
    }

    private static CdcProgressLifecycle readLifecycle(ObjectDataInput in) throws IOException {
        return CdcProgressLifecycle.valueOf(in.readString());
    }

    private static void writeAccuracy(ObjectDataOutput out, CdcProgressAccuracy accuracy)
            throws IOException {
        out.writeString(accuracy.name());
    }

    private static CdcProgressAccuracy readAccuracy(ObjectDataInput in) throws IOException {
        return CdcProgressAccuracy.valueOf(in.readString());
    }

    private static boolean isSupported(CdcProgressAccuracy accuracy) {
        return accuracy == CdcProgressAccuracy.EXACT || accuracy == CdcProgressAccuracy.BEST_EFFORT;
    }

    private static <T> CdcProgressValue<T> supportedValue(CdcProgressAccuracy accuracy, T value) {
        if (accuracy == CdcProgressAccuracy.EXACT) {
            return CdcProgressValue.exact(value);
        }
        return CdcProgressValue.bestEffort(value);
    }

    private static <T> CdcProgressValue<T> emptyValue(CdcProgressAccuracy accuracy) {
        if (accuracy == CdcProgressAccuracy.UNSUPPORTED) {
            return CdcProgressValue.unsupported();
        }
        return CdcProgressValue.unavailable();
    }

    private static void writeNullableLong(ObjectDataOutput out, Long value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            out.writeLong(value);
        }
    }

    private static Long readNullableLong(ObjectDataInput in) throws IOException {
        return in.readBoolean() ? in.readLong() : null;
    }

    private static CdcProgressOwner readOwner(ObjectDataInput in) throws IOException {
        String owner = in.readString();
        try {
            return CdcProgressOwner.valueOf(owner);
        } catch (IllegalArgumentException e) {
            throw new IOException("Unknown CDC progress owner: " + owner, e);
        }
    }

    static int readSize(ObjectDataInput in, String valueName) throws IOException {
        int size = in.readInt();
        if (size < 0) {
            throw new IOException("Invalid CDC progress " + valueName + " count: " + size);
        }
        return size;
    }
}
