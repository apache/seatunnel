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
import org.apache.iceberg.ContentFile;
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

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toList;

@Slf4j
public class IcebergFilesCommitter implements Serializable {

    private static final long serialVersionUID = 1L;

    public static final String SNAPSHOT_PROPERTY_CHECKPOINT_ID = "seatunnel.checkpoint-id";
    static final String SNAPSHOT_PROPERTY_COMMIT_ID = "seatunnel.commit-ids";
    static final String SNAPSHOT_PROPERTY_COMMIT_GROUP = "seatunnel.commit-group";
    static final String SNAPSHOT_PROPERTY_COMMIT_GROUP_COUNT = "seatunnel.commit-group-count";
    private static final String COMMIT_ID_VERSION = "v1:";

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
        doCommit(results, checkpointId, 0, 1);
    }

    public void doCommit(List<WriteResult> results, long checkpointId, int group, int groupCount) {
        if (group < 0 || group >= groupCount) {
            throw new IllegalArgumentException("Invalid Iceberg commit group");
        }
        TableIdentifier tableIdentifier = icebergTableLoader.getTableIdentifier();
        commit(tableIdentifier, results, checkpointId, group, groupCount);
    }

    public boolean isAlreadyCommitted(long checkpointId, List<WriteResult> results) {
        return !fileIds(results).isEmpty()
                && pendingResults(icebergTableLoader.loadTable(), checkpointId, results).isEmpty();
    }

    private List<WriteResult> pendingResults(
            Table table, long checkpointId, List<WriteResult> results) {
        table.refresh();
        Set<String> wantedIds = new HashSet<>();
        for (WriteResult result : results) {
            Set<String> files = fileIds(Collections.singletonList(result));
            if (!files.isEmpty()) {
                wantedIds.add(commitId(files));
            }
        }
        Set<String> publishedIds = new HashSet<>();
        List<Snapshot> legacyCandidates = new ArrayList<>();
        Snapshot head = headSnapshot(table);
        Snapshot current = head;
        boolean historyGap = false;
        while (current != null) {
            String publishedId = current.summary().get(SNAPSHOT_PROPERTY_COMMIT_ID);
            boolean fingerprinted =
                    publishedId != null && publishedId.startsWith(COMMIT_ID_VERSION);
            if (fingerprinted) {
                for (String id : publishedId.substring(COMMIT_ID_VERSION.length()).split(",")) {
                    if (wantedIds.contains(id)) {
                        publishedIds.add(id);
                    }
                }
            }
            // Old checkpoints have no payload fingerprint. A checkpoint number only selects
            // candidates: another job may reuse it, so it is never proof of publication.
            if (!fingerprinted
                    && (checkpointId <= 0
                            || String.valueOf(checkpointId)
                                    .equals(
                                            current.summary()
                                                    .get(SNAPSHOT_PROPERTY_CHECKPOINT_ID)))) {
                legacyCandidates.add(current);
            }
            Long parentId = current.parentId();
            current = parentId != null ? table.snapshot(parentId) : null;
            historyGap |= parentId != null && current == null;
        }
        List<WriteResult> pending = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        Set<String> legacyFiles = null;
        for (WriteResult result : results) {
            Set<String> files = fileIds(Collections.singletonList(result));
            if (!files.isEmpty()) {
                String id = commitId(files);
                if (seen.add(id) && !publishedIds.contains(id)) {
                    if (legacyFiles == null) {
                        legacyFiles = new HashSet<>();
                        for (Snapshot snapshot : legacyCandidates) {
                            for (DataFile file : snapshot.addedDataFiles(table.io())) {
                                legacyFiles.add(fileId(file));
                            }
                            for (DeleteFile file : snapshot.addedDeleteFiles(table.io())) {
                                legacyFiles.add(fileId(file));
                            }
                        }
                        // Without an ID, live files can preserve legacy proof across expiration.
                        // Intact ancestry already supplies it; positive IDs never scan live files.
                        if (head != null && checkpointId <= 0 && historyGap) {
                            addLiveFiles(table, head, legacyFiles);
                        }
                    }
                    if (!alreadyPublished(checkpointId, files, legacyFiles)) {
                        pending.add(result);
                    }
                }
            }
        }
        return pending;
    }

    private static void addLiveFiles(Table table, Snapshot snapshot, Set<String> ids) {
        for (ManifestFile manifest : snapshot.allManifests(table.io())) {
            try {
                if (manifest.content() == ManifestContent.DATA) {
                    try (CloseableIterable<DataFile> files =
                            ManifestFiles.read(manifest, table.io())) {
                        for (DataFile file : files) {
                            ids.add(fileId(file));
                        }
                    }
                } else {
                    try (CloseableIterable<DeleteFile> files =
                            ManifestFiles.readDeleteManifest(manifest, table.io(), table.specs())) {
                        for (DeleteFile file : files) {
                            ids.add(fileId(file));
                        }
                    }
                }
            } catch (IOException e) {
                throw new UncheckedIOException("Cannot establish Iceberg publication state", e);
            }
        }
    }

    private boolean alreadyPublished(
            long checkpointId, Set<String> files, Set<String> publishedFiles) {
        Set<String> remaining = new HashSet<>(files);
        remaining.removeAll(publishedFiles);
        if (remaining.isEmpty()) {
            return true;
        }
        if (remaining.size() != files.size()) {
            throw new IllegalStateException(
                    "Iceberg checkpoint "
                            + checkpointId
                            + " is only partially published; refusing to replay the whole payload");
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

    private static Set<String> fileIds(List<WriteResult> results) {
        Set<String> ids = new HashSet<>();
        for (WriteResult result : results) {
            if (result.getDataFiles() != null) {
                result.getDataFiles().stream()
                        .filter(f -> f.recordCount() > 0)
                        .forEach(f -> ids.add(fileId(f)));
            }
            if (result.getDeleteFiles() != null) {
                result.getDeleteFiles().stream()
                        .filter(f -> f.recordCount() > 0)
                        .forEach(f -> ids.add(fileId(f)));
            }
        }
        return ids;
    }

    private static String commitId(Set<String> files) {
        List<String> sorted = new ArrayList<>(files);
        Collections.sort(sorted);
        return digest(String.join("", sorted).getBytes(StandardCharsets.UTF_8));
    }

    private static String fileId(ContentFile<?> file) {
        // Iceberg file locations are immutable. DVs additionally identify a blob inside a Puffin
        // file; a shared Puffin path alone must not make two different vectors look identical.
        try (ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bytes)) {
            out.writeInt(file.content().id());
            out.writeInt(file.specId());
            writeString(out, file.path().toString());
            writeString(out, file.format().name());
            out.writeLong(file.recordCount());
            out.writeLong(file.fileSizeInBytes());
            if (file instanceof DeleteFile) {
                DeleteFile delete = (DeleteFile) file;
                writeString(out, delete.referencedDataFile());
                writeString(out, String.valueOf(delete.contentOffset()));
                writeString(out, String.valueOf(delete.contentSizeInBytes()));
            }
            return digest(bytes.toByteArray());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        if (value == null) {
            out.writeInt(-1);
        } else {
            byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
            out.writeInt(bytes.length);
            out.write(bytes);
        }
    }

    private static String digest(byte[] bytes) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(bytes);
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte value : hash) {
                hex.append(Character.forDigit((value >>> 4) & 0xf, 16));
                hex.append(Character.forDigit(value & 0xf, 16));
            }
            return hex.toString();
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private void commit(
            TableIdentifier tableIdentifier,
            List<WriteResult> results,
            long checkpointId,
            int group,
            int groupCount) {
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
            List<WriteResult> pending = pendingResults(table, checkpointId, results);
            if (pending.isEmpty()) {
                log.info("Iceberg payload for checkpoint {} is already published", checkpointId);
                return;
            }
            dataFiles =
                    pending.stream()
                            .filter(result -> result.getDataFiles() != null)
                            .flatMap(result -> result.getDataFiles().stream())
                            .filter(file -> file.recordCount() > 0)
                            .collect(toList());
            deleteFiles =
                    pending.stream()
                            .filter(result -> result.getDeleteFiles() != null)
                            .flatMap(result -> result.getDeleteFiles().stream())
                            .filter(file -> file.recordCount() > 0)
                            .collect(toList());
            String ids =
                    COMMIT_ID_VERSION
                            + pending.stream()
                                    .map(
                                            result ->
                                                    commitId(
                                                            fileIds(
                                                                    Collections.singletonList(
                                                                            result))))
                                    .sorted()
                                    .collect(java.util.stream.Collectors.joining(","));
            log.info("do commit table : {}", table.toString());
            if (deleteFiles.isEmpty()) {
                AppendFiles append = table.newAppend();
                append.set(SNAPSHOT_PROPERTY_COMMIT_ID, ids);
                if (groupCount > 1) {
                    append.set(SNAPSHOT_PROPERTY_COMMIT_GROUP, String.valueOf(group));
                    append.set(SNAPSHOT_PROPERTY_COMMIT_GROUP_COUNT, String.valueOf(groupCount));
                }
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
                delta.set(SNAPSHOT_PROPERTY_COMMIT_ID, ids);
                if (groupCount > 1) {
                    delta.set(SNAPSHOT_PROPERTY_COMMIT_GROUP, String.valueOf(group));
                    delta.set(SNAPSHOT_PROPERTY_COMMIT_GROUP_COUNT, String.valueOf(groupCount));
                }
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
