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

package org.apache.seatunnel.connectors.seatunnel.iceberg.sink.commit;

import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergTableLoader;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.sink.writer.WriteResult;

import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DeleteFile;
import org.apache.iceberg.ManifestContent;
import org.apache.iceberg.ManifestFile;
import org.apache.iceberg.ManifestFiles;
import org.apache.iceberg.RowDelta;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.SnapshotRef;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toList;

@Slf4j
public class IcebergFilesCommitter implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String SNAPSHOT_PROPERTY_CHECKPOINT_ID = "seatunnel.checkpoint-id";

    private IcebergTableLoader icebergTableLoader;
    private boolean caseSensitive;
    private String branch;

    private IcebergFilesCommitter(IcebergSinkConfig config, IcebergTableLoader icebergTableLoader) {
        this.icebergTableLoader = icebergTableLoader;
        this.caseSensitive = config.isCaseSensitive();
        this.branch = config.getCommitBranch();
    }

    public static IcebergFilesCommitter of(
            IcebergSinkConfig config, IcebergTableLoader icebergTableLoader) {
        return new IcebergFilesCommitter(config, icebergTableLoader);
    }

    public void doCommit(List<WriteResult> results, long checkpointId) {
        TableIdentifier tableIdentifier = icebergTableLoader.getTableIdentifier();
        commit(tableIdentifier, results, checkpointId);
    }

    public boolean isAlreadyCommitted(long checkpointId, List<WriteResult> results) {
        if (checkpointId > 0) {
            return isCommittedById(checkpointId);
        } else {
            return areFilesAlreadyCommitted(results);
        }
    }

    private boolean isCommittedById(long checkpointId) {
        Table table = icebergTableLoader.loadTable();
        String expected = String.valueOf(checkpointId);
        Snapshot current = headSnapshot(table);
        while (current != null) {
            if (expected.equals(current.summary().get(SNAPSHOT_PROPERTY_CHECKPOINT_ID))) {
                return true;
            }
            Long parentId = current.parentId();
            current = parentId != null ? table.snapshot(parentId) : null;
        }
        return false;
    }

    private Snapshot headSnapshot(Table table) {
        if (branch != null) {
            SnapshotRef ref = table.refs().get(branch);
            return ref != null ? table.snapshot(ref.snapshotId()) : null;
        }
        return table.currentSnapshot();
    }

    private boolean areFilesAlreadyCommitted(List<WriteResult> results) {
        Table table = icebergTableLoader.loadTable();
        Snapshot snapshot = headSnapshot(table);
        if (snapshot == null) {
            return false;
        }

        Set<String> pendingPaths = new HashSet<>();
        for (WriteResult result : results) {
            if (result.getDataFiles() != null) {
                result.getDataFiles().stream()
                        .filter(f -> f.recordCount() > 0)
                        .forEach(f -> pendingPaths.add(f.path().toString()));
            }
            if (result.getDeleteFiles() != null) {
                result.getDeleteFiles().stream()
                        .filter(f -> f.recordCount() > 0)
                        .forEach(f -> pendingPaths.add(f.path().toString()));
            }
        }
        if (pendingPaths.isEmpty()) {
            return false;
        }

        for (ManifestFile manifest : snapshot.allManifests(table.io())) {
            try {
                if (manifest.content() == ManifestContent.DATA) {
                    try (CloseableIterable<DataFile> reader =
                            ManifestFiles.read(manifest, table.io())) {
                        for (DataFile file : reader) {
                            if (pendingPaths.contains(file.path().toString())) {
                                return true;
                            }
                        }
                    }
                } else {
                    try (CloseableIterable<DeleteFile> reader =
                            ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
                        for (DeleteFile file : reader) {
                            if (pendingPaths.contains(file.path().toString())) {
                                return true;
                            }
                        }
                    }
                }
            } catch (IOException e) {
                log.warn(
                        "Failed to read manifest {}, assuming files not yet committed",
                        manifest.path(),
                        e);
            }
        }
        return false;
    }

    private void commit(
            TableIdentifier tableIdentifier, List<WriteResult> results, long checkpointId) {
        List<DataFile> dataFiles =
                results.stream()
                        .filter(payload -> payload.getDataFiles() != null)
                        .flatMap(payload -> payload.getDataFiles().stream())
                        .filter(dataFile -> dataFile.recordCount() > 0)
                        .collect(toList());

        List<DeleteFile> deleteFiles =
                results.stream()
                        .filter(payload -> payload.getDeleteFiles() != null)
                        .flatMap(payload -> payload.getDeleteFiles().stream())
                        .filter(deleteFile -> deleteFile.recordCount() > 0)
                        .collect(toList());

        if (dataFiles.isEmpty() && deleteFiles.isEmpty()) {
            log.info(String.format("Nothing to commit to table %s, skipping", tableIdentifier));
        } else {
            Table table = icebergTableLoader.loadTable();
            log.info("do commit table : {}", table.toString());
            if (deleteFiles.isEmpty()) {
                AppendFiles append = table.newAppend();
                if (branch != null) {
                    append.toBranch(branch);
                }
                if (checkpointId > 0) {
                    append.set(SNAPSHOT_PROPERTY_CHECKPOINT_ID, String.valueOf(checkpointId));
                }
                dataFiles.forEach(append::appendFile);
                append.commit();
            } else {
                RowDelta delta = table.newRowDelta();
                if (branch != null) {
                    delta.toBranch(branch);
                }
                delta.caseSensitive(caseSensitive);
                if (checkpointId > 0) {
                    delta.set(SNAPSHOT_PROPERTY_CHECKPOINT_ID, String.valueOf(checkpointId));
                }
                dataFiles.forEach(delta::addRows);
                deleteFiles.forEach(delta::addDeletes);
                delta.commit();
            }
        }
    }
}
