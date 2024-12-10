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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;
import org.apache.seatunnel.shade.com.google.common.collect.Maps;
import org.apache.seatunnel.shade.com.google.common.collect.Streams;

import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.IcebergEnumerationResult;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.enumerator.IcebergEnumeratorPosition;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.split.IcebergFileScanTaskSplit;

import org.apache.curator.shaded.com.google.common.collect.ListMultimap;
import org.apache.curator.shaded.com.google.common.collect.Multimaps;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.IncrementalAppendScan;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Scan;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableProperties;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.util.PropertyUtil;
import org.apache.iceberg.util.SnapshotUtil;
import org.apache.iceberg.util.StructLikeWrapper;
import org.apache.iceberg.util.TableScanUtil;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

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
        if (table.currentSnapshot() == null && context.isCompactionAction()) {
            return Collections.emptyList();
        }
        long currentSnapshotId = table.currentSnapshot().snapshotId();
        try (CloseableIterable<CombinedScanTask> tasksIterable = planTasks(table, context)) {
            List<IcebergFileScanTaskSplit> splits = new ArrayList<>();
            for (CombinedScanTask combinedScanTask : tasksIterable) {
                for (FileScanTask fileScanTask : combinedScanTask.files()) {
                    splits.add(
                            context.isCompactionAction()
                                    ? new IcebergFileScanTaskSplit(
                                            context.getTablePath(), fileScanTask, currentSnapshotId)
                                    : new IcebergFileScanTaskSplit(
                                            context.getTablePath(), fileScanTask));
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

    public static CloseableIterable<CombinedScanTask> planTasks(
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
            if (context.isCompactionAction()) {
                return getCompactionPlanTasks(table, context);
            } else {
                return getCombinedScanTasks(table, context);
            }
        }
    }

    private static CloseableIterable<CombinedScanTask> getCombinedScanTasks(
            Table table, IcebergScanContext context) {
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

    private static CloseableIterable<CombinedScanTask> getCompactionPlanTasks(
            Table table, IcebergScanContext context) {
        CloseableIterable<FileScanTask> fileScanTasks = null;
        if (table.currentSnapshot() == null) {
            return CloseableIterable.empty();
        }
        long startingSnapshotId = table.currentSnapshot().snapshotId();
        try {
            fileScanTasks =
                    table.newScan()
                            .useSnapshot(startingSnapshotId)
                            .caseSensitive(context.isCaseSensitive())
                            .ignoreResiduals()
                            .filter(
                                    context.getFilter() == null
                                            ? Expressions.alwaysTrue()
                                            : context.getFilter())
                            .planFiles();
        } finally {
            try {
                if (fileScanTasks != null) {
                    fileScanTasks.close();
                }
            } catch (IOException ioe) {
                log.warn("Failed to close task iterable", ioe);
            }
        }

        PartitionSpec spec = table.spec();
        Map<StructLikeWrapper, Collection<FileScanTask>> groupedTasks =
                groupTasksByPartition(spec, fileScanTasks.iterator());
        Map<StructLikeWrapper, Collection<FileScanTask>> filteredGroupedTasks =
                groupedTasks.entrySet().stream()
                        .filter(kv -> kv.getValue().size() > 1)
                        .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

        // Nothing to rewrite if there's only one DataFile in each partition.
        if (filteredGroupedTasks.isEmpty()) {
            return CloseableIterable.empty();
        }

        long splitSize =
                context.getSplitSize() == null
                        ? PropertyUtil.propertyAsLong(
                                table.properties(),
                                TableProperties.SPLIT_SIZE,
                                TableProperties.SPLIT_SIZE_DEFAULT)
                        : context.getSplitSize();
        long targetFileSize =
                PropertyUtil.propertyAsLong(
                        table.properties(),
                        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES,
                        TableProperties.WRITE_TARGET_FILE_SIZE_BYTES_DEFAULT);

        int splitLookback =
                context.getSplitLookback() == null
                        ? PropertyUtil.propertyAsInt(
                                table.properties(),
                                TableProperties.SPLIT_LOOKBACK,
                                TableProperties.SPLIT_LOOKBACK_DEFAULT)
                        : context.getSplitLookback();
        long splitOpenFileCost =
                context.getSplitOpenFileCost() == null
                        ? PropertyUtil.propertyAsLong(
                                table.properties(),
                                TableProperties.SPLIT_OPEN_FILE_COST,
                                TableProperties.SPLIT_OPEN_FILE_COST_DEFAULT)
                        : context.getSplitOpenFileCost();

        long targetSizeInBytes = Math.min(splitSize, targetFileSize);
        // Split and combine tasks under each partition
        List<CombinedScanTask> combinedScanTasks =
                filteredGroupedTasks.values().stream()
                        .map(
                                scanTasks -> {
                                    CloseableIterable<FileScanTask> splitTasks =
                                            TableScanUtil.splitFiles(
                                                    CloseableIterable.withNoopClose(scanTasks),
                                                    targetSizeInBytes);
                                    return TableScanUtil.planTasks(
                                            splitTasks,
                                            targetSizeInBytes,
                                            splitLookback,
                                            splitOpenFileCost);
                                })
                        .flatMap(Streams::stream)
                        .filter(task -> task.files().size() > 1 || isPartialFileScan(task))
                        .collect(Collectors.toList());

        if (combinedScanTasks.isEmpty()) {
            return CloseableIterable.empty();
        }
        return CloseableIterable.withNoopClose(combinedScanTasks);
    }

    private static boolean isPartialFileScan(CombinedScanTask task) {
        if (task.files().size() == 1) {
            FileScanTask fileScanTask = task.files().iterator().next();
            return fileScanTask.file().fileSizeInBytes() != fileScanTask.length();
        } else {
            return false;
        }
    }

    private static Map<StructLikeWrapper, Collection<FileScanTask>> groupTasksByPartition(
            PartitionSpec spec, CloseableIterator<FileScanTask> tasksIter) {
        ListMultimap<StructLikeWrapper, FileScanTask> tasksGroupedByPartition =
                Multimaps.newListMultimap(Maps.newHashMap(), Lists::newArrayList);
        StructLikeWrapper partitionWrapper = StructLikeWrapper.forType(spec.partitionType());
        try (CloseableIterator<FileScanTask> iterator = tasksIter) {
            iterator.forEachRemaining(
                    task -> {
                        StructLikeWrapper structLike =
                                partitionWrapper.copyFor(task.file().partition());
                        tasksGroupedByPartition.put(structLike, task);
                    });
        } catch (IOException e) {
            log.warn("Failed to close task iterator", e);
        }
        return tasksGroupedByPartition.asMap();
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
