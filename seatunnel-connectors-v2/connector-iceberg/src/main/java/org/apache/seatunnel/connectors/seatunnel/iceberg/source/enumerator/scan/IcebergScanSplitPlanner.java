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

package org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.scan;

import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.IcebergEnumerationResult;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.IcebergEnumeratorPosition;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.split.IcebergFileScanTaskSplit;

import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.util.SnapshotUtil;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

@Slf4j
public class IcebergScanSplitPlanner {

    public static IcebergEnumerationResult planStreamSplits(
            Table table,
            IcebergScanContext icebergScanContext,
            IcebergEnumeratorPosition lastPosition) {
        // Load increment files
        table.refresh();

        if (lastPosition == null) {
            return initialStreamSplits(table, icebergScanContext);
        }
        return incrementalStreamSplits(table, icebergScanContext, lastPosition);
    }

    private static IcebergEnumerationResult incrementalStreamSplits(
            Table table,
            IcebergScanContext icebergScanContext,
            IcebergEnumeratorPosition lastPosition) {
        Snapshot currentSnapshot = table.currentSnapshot();
        if (currentSnapshot == null) {
            checkArgument(
                    lastPosition.getSnapshotId() == null,
                    "Invalid last enumerated position for an empty table: not null");
            log.info("Skip incremental scan because table is empty");
            return new IcebergEnumerationResult(
                    Collections.emptyList(), lastPosition, lastPosition);
        } else if (lastPosition.getSnapshotId() != null
                && currentSnapshot.snapshotId() == lastPosition.getSnapshotId()) {
            log.debug(
                    "Current table snapshot is already enumerated: {}",
                    currentSnapshot.snapshotId());
            return new IcebergEnumerationResult(
                    Collections.emptyList(), lastPosition, lastPosition);
        }

        IcebergEnumeratorPosition newPosition =
                new IcebergEnumeratorPosition(
                        currentSnapshot.snapshotId(), currentSnapshot.timestampMillis());
        IcebergScanContext incrementalScan =
                icebergScanContext.copyWithAppendsBetween(
                        lastPosition.getSnapshotId(), currentSnapshot.snapshotId());
        List<IcebergFileScanTaskSplit> splits = planSplits(table, incrementalScan);
        log.info(
                "Discovered {} splits from incremental scan: "
                        + "from snapshot (exclusive) is {}, to snapshot (inclusive) is {}",
                splits.size(),
                lastPosition,
                newPosition);
        return new IcebergEnumerationResult(splits, lastPosition, newPosition);
    }

    private static IcebergEnumerationResult initialStreamSplits(
            Table table, IcebergScanContext icebergScanContext) {
        Optional<Snapshot> startSnapshotOptional =
                getStreamStartSnapshot(table, icebergScanContext);
        if (!startSnapshotOptional.isPresent()) {
            return new IcebergEnumerationResult(
                    Collections.emptyList(), null, IcebergEnumeratorPosition.EMPTY);
        }

        Snapshot startSnapshot = startSnapshotOptional.get();
        List<IcebergFileScanTaskSplit> splits = Collections.emptyList();
        IcebergEnumeratorPosition toPosition = IcebergEnumeratorPosition.EMPTY;
        if (IcebergStreamScanStrategy.TABLE_SCAN_THEN_INCREMENTAL.equals(
                icebergScanContext.getStreamScanStrategy())) {
            splits = planSplits(table, icebergScanContext);
            log.info(
                    "Discovered {} splits from initial batch table scan with snapshot Id {}",
                    splits.size(),
                    startSnapshot.snapshotId());

            toPosition =
                    new IcebergEnumeratorPosition(
                            startSnapshot.snapshotId(), startSnapshot.timestampMillis());
        } else {
            Long parentSnapshotId = startSnapshot.parentId();
            if (parentSnapshotId != null) {
                Snapshot parentSnapshot = table.snapshot(parentSnapshotId);
                Long parentSnapshotTimestampMs =
                        parentSnapshot != null ? parentSnapshot.timestampMillis() : null;
                toPosition =
                        new IcebergEnumeratorPosition(parentSnapshotId, parentSnapshotTimestampMs);
            }
            log.info(
                    "Start incremental scan with start snapshot (inclusive): id = {}, timestamp = {}",
                    startSnapshot.snapshotId(),
                    startSnapshot.timestampMillis());
        }

        return new IcebergEnumerationResult(splits, null, toPosition);
    }

    private static Optional<Snapshot> getStreamStartSnapshot(
            Table table, IcebergScanContext icebergScanContext) {
        switch (icebergScanContext.getStreamScanStrategy()) {
            case TABLE_SCAN_THEN_INCREMENTAL:
            case FROM_LATEST_SNAPSHOT:
                return Optional.ofNullable(table.currentSnapshot());
            case FROM_EARLIEST_SNAPSHOT:
                return Optional.ofNullable(SnapshotUtil.oldestAncestor(table));
            case FROM_SNAPSHOT_ID:
                return Optional.of(table.snapshot(icebergScanContext.getStartSnapshotId()));
            case FROM_SNAPSHOT_TIMESTAMP:
                long snapshotIdAsOfTime =
                        SnapshotUtil.snapshotIdAsOfTime(
                                table, icebergScanContext.getStartSnapshotTimestamp());
                Snapshot matchedSnapshot = table.snapshot(snapshotIdAsOfTime);
                if (matchedSnapshot.timestampMillis()
                        == icebergScanContext.getStartSnapshotTimestamp()) {
                    return Optional.of(matchedSnapshot);
                } else {
                    return Optional.of(SnapshotUtil.snapshotAfter(table, snapshotIdAsOfTime));
                }
            default:
                throw new IcebergConnectorException(
                        CommonErrorCodeDeprecated.UNSUPPORTED_OPERATION,
                        "Unsupported stream scan strategy: "
                                + icebergScanContext.getStreamScanStrategy());
        }
    }

    public static List<IcebergFileScanTaskSplit> planSplits(
            Table table, IcebergScanContext context) {
        try (CloseableIterable<CombinedScanTask> tasksIterable = planTasks(table, context)) {
            List<IcebergFileScanTaskSplit> splits = new ArrayList<>();
            for (CombinedScanTask combinedScanTask : tasksIterable) {
                for (FileScanTask fileScanTask : combinedScanTask.files()) {
                    splits.add(new IcebergFileScanTaskSplit(context.getTablePath(), fileScanTask));
                }
            }
            return splits;
        } catch (IOException e) {
            throw new IcebergConnectorException(
                    IcebergConnectorErrorCode.FILE_SCAN_SPLIT_FAILED,
                    "Failed to scan iceberg splits from: " + table.name(),
                    e);
        }
    }

    private static CloseableIterable<CombinedScanTask> planTasks(
            Table table, IcebergScanContext context) {
        if (context.isStreaming()
                || context.getStartSnapshotId() != null
                || context.getEndSnapshotId() != null) {
            IncrementalAppendScan scan = table.newIncrementalAppendScan();
            scan = rebuildScanWithBaseConfig(scan, context);
            if (context.getStartSnapshotId() != null) {
                scan = scan.fromSnapshotExclusive(context.getStartSnapshotId());
            }
            if (context.getEndSnapshotId() != null) {
                scan = scan.toSnapshot(context.getEndSnapshotId());
            }
            return scan.planTasks();
        } else {
            TableScan scan = table.newScan();
            scan = rebuildScanWithBaseConfig(scan, context);
            if (context.getUseSnapshotId() != null) {
                scan = scan.useSnapshot(context.getUseSnapshotId());
            }
            if (context.getUseSnapshotTimestamp() != null) {
                scan = scan.asOfTime(context.getUseSnapshotTimestamp());
            }
            return scan.planTasks();
        }
    }

    private static <T extends Scan<T, FileScanTask, CombinedScanTask>> T rebuildScanWithBaseConfig(
            T scan, IcebergScanContext context) {
        T newScan = scan.caseSensitive(context.isCaseSensitive()).project(context.getSchema());
        if (context.getFilter() != null) {
            newScan = newScan.filter(context.getFilter());
        }
        if (context.getSplitSize() != null) {
            newScan = newScan.option(TableProperties.SPLIT_SIZE, context.getSplitSize().toString());
        }
        if (context.getSplitLookback() != null) {
            newScan =
                    newScan.option(
                            TableProperties.SPLIT_LOOKBACK, context.getSplitLookback().toString());
        }
        if (context.getSplitOpenFileCost() != null) {
            newScan =
                    newScan.option(
                            TableProperties.SPLIT_OPEN_FILE_COST,
                            context.getSplitOpenFileCost().toString());
        }
        return newScan;
    }
}
