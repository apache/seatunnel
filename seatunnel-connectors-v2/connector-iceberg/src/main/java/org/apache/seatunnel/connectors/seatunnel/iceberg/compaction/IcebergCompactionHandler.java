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

package org.apache.seatunnel.connectors.seatunnel.iceberg.compaction;

import org.apache.seatunnel.shade.com.google.common.collect.Iterables;
import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.RecordWriter;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.WriteResult;
import org.apache.seatunnel.connectors.seatunnel.iceberg.source.split.IcebergFileScanTaskSplit;

import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.RewriteFiles;
import org.apache.iceberg.Table;
import org.apache.iceberg.exceptions.CommitStateUnknownException;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.util.Tasks;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
public class IcebergCompactionHandler {

    public static final String COMPACTION_ACTION_MESSAGE = "compaction_end";
    public static final String CURRENT_SNAPSHOT_ID = "current_snapshot_id";
    public static final String DELETED_FILES = "deleted_files";

    private final Table table;
    private final FileIO fileIO;
    private final RecordWriter writer;

    public IcebergCompactionHandler(IcebergTableLoader icebergTableLoader, RecordWriter writer) {
        this.table = icebergTableLoader.loadTable();
        this.fileIO = this.table.io();
        this.writer = writer;
    }

    public static void emitCompactionEndRecord(
            Collector<SeaTunnelRow> output, List<IcebergFileScanTaskSplit> finishedSplits) {
        if (finishedSplits.isEmpty()) {
            log.warn("No compaction split, skip it.");
            return;
        }
        List<String> deletedFiles =
                finishedSplits.stream()
                        .map(split -> split.getTask().file().path().toString())
                        .collect(Collectors.toList());

        Optional<IcebergFileScanTaskSplit> firstSplit = finishedSplits.stream().findFirst();
        if (!firstSplit.isPresent()) {
            log.warn("Failed to find any splits, skipping compaction end record emission.");
            return;
        }
        IcebergFileScanTaskSplit oneSplit = firstSplit.get();
        SeaTunnelRow compactionEndRecord = new SeaTunnelRow(0);
        compactionEndRecord.setTableId(oneSplit.getTablePath().getFullName());

        Map<String, Object> options = new HashMap<>();
        options.put(COMPACTION_ACTION_MESSAGE, COMPACTION_ACTION_MESSAGE);
        options.put(CURRENT_SNAPSHOT_ID, oneSplit.getCurrentSnapshotId());
        options.put(DELETED_FILES, deletedFiles);
        compactionEndRecord.setOptions(options);
        output.collect(compactionEndRecord);
    }

    public boolean isCompactionEndRecord(SeaTunnelRow element) {
        if (element == null
                || element.getOptions().isEmpty()
                || !element.getOptions().containsKey(COMPACTION_ACTION_MESSAGE)) {
            return false;
        }

        Map<String, Object> options = element.getOptions();
        Object end = options.get(COMPACTION_ACTION_MESSAGE);

        if (end instanceof String) {
            return COMPACTION_ACTION_MESSAGE.equals(end);
        }
        return false;
    }

    public void doCompaction(SeaTunnelRow element) throws IOException {
        Long startingSnapshotId = getStartSnapshotId(element.getOptions(), CURRENT_SNAPSHOT_ID);
        List<String> deletedFileNames = getSequenceNumbers(element.getOptions(), DELETED_FILES);
        List<DataFile> deletedDataFiles =
                deletedDataFiles(startingSnapshotId, new HashSet<>(deletedFileNames));
        List<DataFile> addedDataFiles = collectAddedDataFiles();

        replaceDataFiles(deletedDataFiles, addedDataFiles, startingSnapshotId);
    }

    private Long getStartSnapshotId(Map<String, Object> options, String key) {
        if (!options.containsKey(key)) {
            throw new IllegalArgumentException("Expected key " + key + " not found in options");
        }
        Object field = options.get(key);
        if (field == null) {
            throw new IllegalArgumentException("Expected non-null value for key " + key);
        }
        if (!(field instanceof Long)) {
            throw new IllegalArgumentException("Expected Long type for key " + key);
        }
        return (Long) field;
    }

    private List<String> getSequenceNumbers(Map<String, Object> options, String key) {
        if (!options.containsKey(key)) {
            throw new IllegalArgumentException("Expected key " + key + " not found in options");
        }
        Object field = options.get(key);
        if (field == null) {
            throw new IllegalArgumentException("Expected non-null value for key " + key);
        }
        if (!(field instanceof List)) {
            throw new IllegalArgumentException("Expected List type for key " + key);
        }
        return (List<String>) field;
    }

    private List<DataFile> deletedDataFiles(Long startingSnapshotId, Set<String> deletedFileNames)
            throws IOException {
        List<DataFile> currentDataFiles = Lists.newArrayList();
        try (CloseableIterator<CombinedScanTask> combinedScanTasks =
                table.newScan().useSnapshot(startingSnapshotId).planTasks().iterator()) {
            while (combinedScanTasks.hasNext()) {
                CombinedScanTask combinedScanTask = combinedScanTasks.next();
                for (FileScanTask fileScanTask : combinedScanTask.files()) {
                    String path = fileScanTask.file().path().toString();
                    if (deletedFileNames.contains(path)) {
                        currentDataFiles.add(fileScanTask.file());
                    }
                }
            }
        }
        return currentDataFiles;
    }

    private List<DataFile> collectAddedDataFiles() throws IOException {
        List<DataFile> addedDataFiles = Lists.newArrayList();
        List<WriteResult> writeResults = writer.complete();
        for (WriteResult result : writeResults) {
            addedDataFiles.addAll(result.getDataFiles());
        }
        return addedDataFiles;
    }

    private void replaceDataFiles(
            Iterable<DataFile> deletedDataFiles,
            Iterable<DataFile> addedDataFiles,
            long startingSnapshotId) {
        try {
            doReplace(deletedDataFiles, addedDataFiles, startingSnapshotId);
        } catch (CommitStateUnknownException e) {
            log.warn("Commit state unknown, cannot clean up files that may have been committed", e);
            throw e;
        } catch (Exception e) {
            log.warn("Failed to commit rewrite, cleaning up rewritten files", e);
            cleanupRewrittenFiles(addedDataFiles);
            throw e;
        }
    }

    private void doReplace(
            Iterable<DataFile> deletedDataFiles,
            Iterable<DataFile> addedDataFiles,
            long startingSnapshotId) {
        RewriteFiles rewriteFiles = table.newRewrite().validateFromSnapshot(startingSnapshotId);
        for (DataFile dataFile : deletedDataFiles) {
            rewriteFiles.deleteFile(dataFile);
            log.info("Deleted file: {}", dataFile.path().toString());
        }
        for (DataFile dataFile : addedDataFiles) {
            rewriteFiles.addFile(dataFile);
            log.info("Added file: {}", dataFile.path().toString());
        }
        long sequenceNumber = table.snapshot(startingSnapshotId).sequenceNumber();
        rewriteFiles.dataSequenceNumber(sequenceNumber);
        rewriteFiles.commit();
    }

    private void cleanupRewrittenFiles(Iterable<DataFile> addedDataFiles) {
        Tasks.foreach(
                        Iterables.transform(
                                addedDataFiles, contentFile -> contentFile.path().toString()))
                .noRetry()
                .suppressFailureWhenFinished()
                .onFailure((location, exc) -> log.warn("Failed to delete: {}", location, exc))
                .run(fileIO::deleteFile);
    }
}
