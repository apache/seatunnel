/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.checkpoint.storage.hdfs;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.AbstractCheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.hdfs.common.AbstractConfiguration;
import org.apache.seatunnel.engine.checkpoint.storage.hdfs.common.FileConfiguration;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointData;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointHandle;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointManifestEntry;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointManifestValidator;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointMeta;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointRequest;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorageUtils;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.seatunnel.engine.checkpoint.storage.constants.StorageConstants.STORAGE_NAME_SPACE;
import static org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorageConstants.META_FILE_NAME;
import static org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorageConstants.PAYLOAD_FORMAT_V1;
import static org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorageConstants.SAVEPOINT_ROOT_DIR;
import static org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorageConstants.STAGING_DIR;

@Slf4j
public class HdfsStorage extends AbstractCheckpointStorage implements SavepointStorage {

    public FileSystem fs;
    private static final String STORAGE_TMP_SUFFIX = "tmp";
    private static final String STORAGE_TYPE_KEY = "storage.type";

    public HdfsStorage(Map<String, String> configuration) throws CheckpointStorageException {
        this.initStorage(configuration);
    }

    @Override
    public void initStorage(Map<String, String> configuration) throws CheckpointStorageException {
        if (StringUtils.isNotBlank(configuration.get(STORAGE_NAME_SPACE))) {
            setStorageNameSpace(configuration.get(STORAGE_NAME_SPACE));
            configuration.remove(STORAGE_NAME_SPACE);
        }
        Configuration hadoopConf = getConfiguration(configuration);
        try {
            fs = FileSystem.get(hadoopConf);
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to get file system", e);
        }
    }

    private Configuration getConfiguration(Map<String, String> config)
            throws CheckpointStorageException {
        String storageType =
                config.getOrDefault(STORAGE_TYPE_KEY, FileConfiguration.LOCAL.toString());
        config.remove(STORAGE_TYPE_KEY);
        AbstractConfiguration configuration =
                FileConfiguration.valueOf(storageType.toUpperCase()).getConfiguration();
        return configuration.buildConfiguration(config);
    }

    @Override
    public String storeCheckPoint(PipelineState state) throws CheckpointStorageException {
        byte[] datas;
        try {
            datas = serializeCheckPointData(state);
        } catch (IOException e) {
            throw new CheckpointStorageException(
                    String.format("Failed to serialize checkpoint data, state: %s", state), e);
        }
        Path filePath =
                new Path(
                        getStorageParentDirectory()
                                + state.getJobId()
                                + "/"
                                + getCheckPointName(state));

        Path tmpFilePath =
                new Path(
                        getStorageParentDirectory()
                                + state.getJobId()
                                + "/"
                                + getCheckPointName(state)
                                + STORAGE_TMP_SUFFIX);
        try (FSDataOutputStream out = fs.create(tmpFilePath, false)) {
            out.write(datas);
        } catch (IOException e) {
            throw new CheckpointStorageException(
                    String.format(
                            "Failed to write checkpoint data, file: %s, state: %s",
                            tmpFilePath, state),
                    e);
        }
        try {
            boolean success = fs.rename(tmpFilePath, filePath);
            if (!success) {
                throw new CheckpointStorageException("Failed to rename tmp file to final file");
            }

        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to rename tmp file to final file");
        } finally {
            try {
                // clean up tmp file, if still lying around
                if (fs.exists(tmpFilePath)) {
                    fs.delete(tmpFilePath, false);
                }
            } catch (IOException ioe) {
                log.error("Failed to delete tmp file", ioe);
            }
        }

        return filePath.getName();
    }

    @Override
    public List<PipelineState> getAllCheckpoints(String jobId) throws CheckpointStorageException {
        String path = getStorageParentDirectory() + jobId;
        List<String> fileNames = getFileNames(path);
        if (fileNames.isEmpty()) {
            log.info("No checkpoint found for this job, the job id is: " + jobId);
            return new ArrayList<>();
        }
        List<PipelineState> states = new ArrayList<>();
        fileNames.forEach(
                file -> {
                    try {
                        states.add(readPipelineState(file, jobId));
                    } catch (CheckpointStorageException e) {
                        log.error("Failed to read checkpoint data from file: " + file, e);
                    }
                });
        if (states.isEmpty()) {
            throw new CheckpointStorageException(
                    "No checkpoint found for job, job id is: " + jobId);
        }
        return states;
    }

    @Override
    public List<PipelineState> getLatestCheckpoint(String jobId) throws CheckpointStorageException {
        String path = getStorageParentDirectory() + jobId;
        List<String> fileNames = getFileNames(path);
        if (fileNames.isEmpty()) {
            log.info("No checkpoint found for this  job, the job id is: " + jobId);
            return new ArrayList<>();
        }
        Set<String> latestPipelineNames = getLatestPipelineNames(fileNames);
        List<PipelineState> latestPipelineStates = new ArrayList<>();
        latestPipelineNames.forEach(
                fileName -> {
                    try {
                        latestPipelineStates.add(readPipelineState(fileName, jobId));
                    } catch (CheckpointStorageException e) {
                        log.error("Failed to read pipeline state for file: {}", fileName, e);
                    }
                });

        if (latestPipelineStates.isEmpty()) {
            log.info("No checkpoint found for this job, the job id:{} ", jobId);
        }
        return latestPipelineStates;
    }

    @Override
    public PipelineState getLatestCheckpointByJobIdAndPipelineId(String jobId, String pipelineId)
            throws CheckpointStorageException {
        String path = getStorageParentDirectory() + jobId;
        List<String> fileNames = getFileNames(path);
        if (fileNames.isEmpty()) {
            log.info("No checkpoint found for job, job id is: " + jobId);
            return null;
        }

        String latestFileName =
                getLatestCheckpointFileNameByJobIdAndPipelineId(fileNames, pipelineId);
        if (latestFileName == null) {
            log.info(
                    "No checkpoint found for this job, the job id is: "
                            + jobId
                            + ", pipeline id is: "
                            + pipelineId);
            return null;
        }
        return readPipelineState(latestFileName, jobId);
    }

    @Override
    public List<PipelineState> getCheckpointsByJobIdAndPipelineId(String jobId, String pipelineId)
            throws CheckpointStorageException {
        String path = getStorageParentDirectory() + jobId;
        List<String> fileNames = getFileNames(path);
        if (fileNames.isEmpty()) {
            log.info("No checkpoint found for this job, the job id is: " + jobId);
            return new ArrayList<>();
        }

        List<PipelineState> pipelineStates = new ArrayList<>();
        fileNames.forEach(
                file -> {
                    String filePipelineId = getPipelineIdByFileName(file);
                    if (pipelineId.equals(filePipelineId)) {
                        try {
                            pipelineStates.add(readPipelineState(file, jobId));
                        } catch (Exception e) {
                            log.error("Failed to read checkpoint data from file " + file, e);
                        }
                    }
                });
        return pipelineStates;
    }

    @Override
    public void deleteCheckpoint(String jobId) {
        String jobPath = getStorageParentDirectory() + jobId;
        try {
            fs.delete(new Path(jobPath), true);
        } catch (IOException e) {
            log.warn("Failed to delete checkpoint for job {}", jobId, e);
        }
    }

    @Override
    public PipelineState getCheckpoint(String jobId, String pipelineId, String checkpointId)
            throws CheckpointStorageException {
        String path = getStorageParentDirectory() + jobId;
        List<String> fileNames = getFileNames(path);
        if (fileNames.isEmpty()) {
            log.info("No checkpoint found for this job,  the job id is: " + jobId);
            return null;
        }
        for (String fileName : fileNames) {
            if (pipelineId.equals(getPipelineIdByFileName(fileName))
                    && checkpointId.equals(getCheckpointIdByFileName(fileName))) {
                try {
                    return readPipelineState(fileName, jobId);
                } catch (Exception e) {
                    log.error(
                            "Failed to get checkpoint {} for job {}, pipeline {}",
                            checkpointId,
                            jobId,
                            pipelineId,
                            e);
                }
            }
        }
        throw new CheckpointStorageException(
                String.format(
                        "No checkpoint found, job(%s), pipeline(%s), checkpoint(%s)",
                        jobId, pipelineId, checkpointId));
    }

    @Override
    public synchronized void deleteCheckpoint(String jobId, String pipelineId, String checkpointId)
            throws CheckpointStorageException {
        String path = getStorageParentDirectory() + jobId;
        List<String> fileNames = getFileNames(path);
        if (fileNames.isEmpty()) {
            throw new CheckpointStorageException(
                    "No checkpoint found for job, job id is: " + jobId);
        }
        fileNames.forEach(
                fileName -> {
                    if (pipelineId.equals(getPipelineIdByFileName(fileName))
                            && checkpointId.equals(getCheckpointIdByFileName(fileName))) {
                        try {
                            fs.delete(
                                    new Path(path + DEFAULT_CHECKPOINT_FILE_PATH_SPLIT + fileName),
                                    false);
                        } catch (Exception e) {
                            log.error(
                                    "Failed to delete checkpoint {} for job {}, pipeline {}",
                                    checkpointId,
                                    jobId,
                                    pipelineId,
                                    e);
                        }
                    }
                });
    }

    @Override
    public void deleteCheckpoint(String jobId, String pipelineId, List<String> checkpointIdList)
            throws CheckpointStorageException {
        String path = getStorageParentDirectory() + jobId;
        List<String> fileNames = getFileNames(path);
        if (fileNames.isEmpty()) {
            throw new CheckpointStorageException(
                    "No checkpoint found for job, job id is: " + jobId);
        }
        fileNames.forEach(
                fileName -> {
                    String checkpointIdByFileName = getCheckpointIdByFileName(fileName);
                    if (pipelineId.equals(getPipelineIdByFileName(fileName))
                            && checkpointIdList.contains(checkpointIdByFileName)) {
                        try {
                            fs.delete(
                                    new Path(path + DEFAULT_CHECKPOINT_FILE_PATH_SPLIT + fileName),
                                    false);
                        } catch (Exception e) {
                            log.error(
                                    "Failed to delete checkpoint {} for job {}, pipeline {}",
                                    checkpointIdByFileName,
                                    jobId,
                                    pipelineId,
                                    e);
                        }
                    }
                });
    }

    public List<String> getFileNames(String path) throws CheckpointStorageException {
        try {
            Path parentPath = new Path(path);
            if (!fs.exists(parentPath)) {
                log.info("Path " + path + " is not a directory");
                return new ArrayList<>();
            }
            FileStatus[] fileStatus =
                    fs.listStatus(parentPath, path1 -> path1.getName().endsWith(FILE_FORMAT));
            List<String> fileNames = new ArrayList<>();
            for (FileStatus status : fileStatus) {
                fileNames.add(status.getPath().getName());
            }
            return fileNames;
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to list files from names" + path, e);
        }
    }

    /**
     * Get checkpoint name
     *
     * @param fileName file name
     * @return checkpoint data
     */
    private PipelineState readPipelineState(String fileName, String jobId)
            throws CheckpointStorageException {
        fileName =
                getStorageParentDirectory() + jobId + DEFAULT_CHECKPOINT_FILE_PATH_SPLIT + fileName;
        try (FSDataInputStream in = fs.open(new Path(fileName));
                ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            IOUtils.copyBytes(in, stream, 1024);
            byte[] bytes = stream.toByteArray();
            return deserializeCheckPointData(bytes);
        } catch (IOException e) {
            throw new CheckpointStorageException(
                    String.format(
                            "Failed to read checkpoint data, file name is %s,job id is %s",
                            fileName, jobId),
                    e);
        }
    }

    // ------------------------------------------------------------------------
    // SavepointStorage capability
    // ------------------------------------------------------------------------

    @Override
    public SavepointWriter beginSavepoint(SavepointRequest request) {
        return new HdfsSavepointWriter(request);
    }

    @Override
    public List<SavepointHandle> listCompletedSavepoints(String jobId)
            throws CheckpointStorageException {
        try {
            Path jobDir = savepointJobDirectory(jobId);
            if (!fs.exists(jobDir)) {
                return new ArrayList<>();
            }
            List<SavepointHandle> handles = new ArrayList<>();
            for (FileStatus status : fs.listStatus(jobDir)) {
                if (!status.isDirectory() || STAGING_DIR.equals(status.getPath().getName())) {
                    continue;
                }
                Path metaPath = new Path(status.getPath(), META_FILE_NAME);
                if (!fs.exists(metaPath)) {
                    continue;
                }
                try {
                    SavepointMeta meta = SavepointStorageUtils.deserializeMeta(readBytes(metaPath));
                    SavepointStorageUtils.verifyManifestChecksum(meta);
                    if (!meta.getSavepointId().equals(status.getPath().getName())) {
                        continue;
                    }
                    if (!manifestFilesComplete(status.getPath(), meta)) {
                        continue;
                    }
                    handles.add(
                            new SavepointHandle(
                                    jobId,
                                    meta.getSavepointId(),
                                    meta.getFormatVersion(),
                                    meta.getTriggerTimestamp(),
                                    meta.getPipelines().size()));
                } catch (Exception e) {
                    log.warn("Skip unreadable savepoint bundle {}", status.getPath(), e);
                }
            }
            handles.sort(
                    Comparator.comparingLong(SavepointHandle::getTriggerTimestamp)
                            .reversed()
                            .thenComparing(
                                    SavepointHandle::getSavepointId, Comparator.reverseOrder()));
            return handles;
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to list savepoints for job " + jobId, e);
        }
    }

    @Override
    public SavepointData readSavepoint(String jobId, String savepointId)
            throws CheckpointStorageException {
        try {
            Path dir = savepointDirectory(jobId, savepointId);
            Path metaPath = new Path(dir, META_FILE_NAME);
            if (!fs.exists(metaPath)) {
                throw new CheckpointStorageException(
                        "Savepoint not found: job " + jobId + ", savepoint " + savepointId);
            }
            SavepointMeta meta = SavepointStorageUtils.deserializeMeta(readBytes(metaPath));
            SavepointStorageUtils.verifyManifestChecksum(meta);
            SavepointManifestValidator.validateMetadata(meta, jobId, savepointId);
            Map<Integer, PipelineState> pipelineStates = new HashMap<>();
            for (SavepointManifestEntry entry : meta.getPipelines()) {
                Path payload = new Path(dir, entry.getPayloadFile());
                byte[] bytes = readBytes(payload);
                if (bytes.length != entry.getPayloadLength()) {
                    throw new CheckpointStorageException(
                            "Savepoint payload length mismatch for job "
                                    + jobId
                                    + ", savepoint "
                                    + savepointId
                                    + ", pipeline "
                                    + entry.getPipelineId()
                                    + ": expected "
                                    + entry.getPayloadLength()
                                    + ", got "
                                    + bytes.length);
                }
                if (!SavepointStorageUtils.sha256Hex(bytes).equals(entry.getPayloadChecksum())) {
                    throw new CheckpointStorageException(
                            "Savepoint payload checksum mismatch for job "
                                    + jobId
                                    + ", savepoint "
                                    + savepointId
                                    + ", pipeline "
                                    + entry.getPipelineId());
                }
                pipelineStates.put(entry.getPipelineId(), deserializeCheckPointData(bytes));
            }
            SavepointManifestValidator.validate(meta, jobId, savepointId, pipelineStates);
            return new SavepointData(jobId, savepointId, meta, pipelineStates);
        } catch (IOException e) {
            throw new CheckpointStorageException(
                    "Failed to read savepoint " + savepointId + " for job " + jobId, e);
        }
    }

    @Override
    public void deleteSavepoint(String jobId, String savepointId)
            throws CheckpointStorageException {
        try {
            Path dir = savepointDirectory(jobId, savepointId);
            if (fs.exists(dir)) {
                fs.delete(dir, true);
            }
        } catch (IOException e) {
            throw new CheckpointStorageException(
                    "Failed to delete savepoint " + savepointId + " for job " + jobId, e);
        }
    }

    @Override
    public void deleteSavepoints(String jobId) throws CheckpointStorageException {
        try {
            Path jobDir = savepointJobDirectory(jobId);
            if (fs.exists(jobDir)) {
                fs.delete(jobDir, true);
            }
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to delete savepoints for job " + jobId, e);
        }
    }

    private Path savepointRoot() {
        return new Path(getStorageParentDirectory(), SAVEPOINT_ROOT_DIR);
    }

    private Path savepointJobDirectory(String jobId) {
        return new Path(savepointRoot(), jobId);
    }

    private Path savepointDirectory(String jobId, String savepointId) {
        return new Path(savepointJobDirectory(jobId), savepointId);
    }

    private boolean manifestFilesComplete(Path dir, SavepointMeta meta) {
        for (SavepointManifestEntry entry : meta.getPipelines()) {
            try {
                Path payload = new Path(dir, entry.getPayloadFile());
                if (!fs.exists(payload)
                        || fs.getFileStatus(payload).getLen() != entry.getPayloadLength()) {
                    return false;
                }
            } catch (IOException e) {
                return false;
            }
        }
        return true;
    }

    private byte[] readBytes(Path path) throws IOException {
        try (FSDataInputStream in = fs.open(path);
                ByteArrayOutputStream stream = new ByteArrayOutputStream()) {
            IOUtils.copyBytes(in, stream, 1024);
            return stream.toByteArray();
        }
    }

    /** Savepoint writer implemented on top of the Hadoop file system. */
    private final class HdfsSavepointWriter implements SavepointWriter {

        private final SavepointRequest request;
        private final Path stagingDir;
        /** File name -> serialized payload bytes written to staging. */
        private final Map<String, byte[]> written = new LinkedHashMap<>();

        private final Map<String, Integer> pipelineIds = new HashMap<>();
        private final Map<String, Long> checkpointIds = new HashMap<>();

        HdfsSavepointWriter(SavepointRequest request) {
            this.request = request;
            this.stagingDir =
                    new Path(
                            new Path(new Path(savepointRoot(), request.getJobId()), STAGING_DIR),
                            request.getAttemptId());
        }

        /**
         * Write one pipeline payload. Synchronized because all pipeline coordinators of the job
         * share this writer and call it concurrently - the in-memory manifest state must be
         * consistent.
         */
        @Override
        public synchronized void writePipeline(PipelineState state)
                throws CheckpointStorageException {
            byte[] data;
            try {
                data = serializeCheckPointData(state);
            } catch (IOException e) {
                throw new CheckpointStorageException(
                        "Failed to serialize savepoint pipeline data", e);
            }
            String fileName =
                    SavepointStorageUtils.pipelinePayloadFileName(
                            state.getPipelineId(), state.getCheckpointId());
            Path tmp = new Path(stagingDir, fileName + STORAGE_TMP_SUFFIX);
            Path out = new Path(stagingDir, fileName);
            try {
                fs.mkdirs(stagingDir);
                try (FSDataOutputStream outStream = fs.create(tmp, false)) {
                    outStream.write(data);
                }
                boolean success = fs.rename(tmp, out);
                if (!success) {
                    throw new CheckpointStorageException(
                            "Failed to rename savepoint payload tmp file to " + out);
                }
            } catch (IOException e) {
                throw new CheckpointStorageException("Failed to write savepoint payload " + out, e);
            }
            written.put(fileName, data);
            pipelineIds.put(fileName, state.getPipelineId());
            checkpointIds.put(fileName, state.getCheckpointId());
        }

        @Override
        public synchronized void commitSavepoint(SavepointMeta meta)
                throws CheckpointStorageException {
            if (written.isEmpty()) {
                throw new CheckpointStorageException(
                        "No pipeline payload written for savepoint " + request.getSavepointId());
            }
            if (!request.getSavepointId().equals(meta.getSavepointId())) {
                throw new CheckpointStorageException(
                        "Savepoint id mismatch: request "
                                + request.getSavepointId()
                                + " vs metadata "
                                + meta.getSavepointId());
            }
            validateCompleteBundle();
            Path finalDir = savepointDirectory(request.getJobId(), request.getSavepointId());
            try {
                if (fs.exists(new Path(finalDir, META_FILE_NAME))) {
                    throw new CheckpointStorageException(
                            "Savepoint already exists: job "
                                    + request.getJobId()
                                    + ", savepoint "
                                    + request.getSavepointId());
                }
                List<SavepointManifestEntry> entries = new ArrayList<>();
                written.forEach(
                        (fileName, data) ->
                                entries.add(
                                        new SavepointManifestEntry(
                                                pipelineIds.get(fileName),
                                                checkpointIds.get(fileName),
                                                fileName,
                                                data.length,
                                                SavepointStorageUtils.sha256Hex(data),
                                                PAYLOAD_FORMAT_V1)));
                meta.setPipelines(entries);
                SavepointStorageUtils.verifyManifestComplete(request.getSavepointId(), entries);
                meta.setManifestChecksum(SavepointStorageUtils.manifestChecksum(entries));

                fs.mkdirs(finalDir);
                FileStatus[] staged =
                        fs.exists(stagingDir) ? fs.listStatus(stagingDir) : new FileStatus[0];
                for (FileStatus status : staged) {
                    if (status.getPath().getName().endsWith(STORAGE_TMP_SUFFIX)) {
                        continue;
                    }
                    Path target = new Path(finalDir, status.getPath().getName());
                    if (fs.exists(target)) {
                        fs.delete(target, false);
                    }
                    boolean success = fs.rename(status.getPath(), target);
                    if (!success) {
                        throw new CheckpointStorageException(
                                "Failed to move savepoint payload "
                                        + status.getPath()
                                        + " to "
                                        + target);
                    }
                }
                Path metaTmp = new Path(finalDir, META_FILE_NAME + STORAGE_TMP_SUFFIX);
                try (FSDataOutputStream outStream = fs.create(metaTmp, true)) {
                    outStream.write(SavepointStorageUtils.serializeMeta(meta));
                }
                if (fs.exists(new Path(finalDir, META_FILE_NAME))) {
                    fs.delete(new Path(finalDir, META_FILE_NAME), false);
                }
                boolean success = fs.rename(metaTmp, new Path(finalDir, META_FILE_NAME));
                if (!success) {
                    throw new CheckpointStorageException(
                            "Failed to rename savepoint metadata tmp file for "
                                    + request.getSavepointId());
                }
                if (fs.exists(stagingDir)) {
                    fs.delete(stagingDir, true);
                }
            } catch (CheckpointStorageException e) {
                throw e;
            } catch (IOException e) {
                throw new CheckpointStorageException(
                        "Failed to commit savepoint " + request.getSavepointId(), e);
            }
        }

        /** Rejects a commit that does not cover exactly the expected pipelines (if declared). */
        private void validateCompleteBundle() throws CheckpointStorageException {
            Set<Integer> actualPipelineIds = new HashSet<>(pipelineIds.values());
            SavepointStorageUtils.verifyCompleteBundle(
                    request.getSavepointId(), actualPipelineIds, request.getExpectedPipelineIds());
        }

        @Override
        public synchronized void abortSavepoint() {
            try {
                if (fs.exists(stagingDir)) {
                    fs.delete(stagingDir, true);
                }
            } catch (IOException e) {
                log.warn("Failed to abort savepoint staging {}", stagingDir, e);
            }
        }
    }
}
