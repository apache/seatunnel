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

package org.apache.seatunnel.engine.checkpoint.storage.localfile;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;
import org.apache.seatunnel.shade.org.apache.commons.lang3.exception.ExceptionUtils;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.AbstractCheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointData;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointHandle;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointManifestEntry;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointManifestValidator;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointMeta;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointRequest;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorageUtils;
import org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointWriter;

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.checkpoint.storage.constants.StorageConstants.STORAGE_NAME_SPACE;
import static org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorageConstants.META_FILE_NAME;
import static org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorageConstants.PAYLOAD_FORMAT_V1;
import static org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorageConstants.SAVEPOINT_ROOT_DIR;
import static org.apache.seatunnel.engine.checkpoint.storage.savepoint.SavepointStorageConstants.STAGING_DIR;

@Slf4j
public class LocalFileStorage extends AbstractCheckpointStorage implements SavepointStorage {

    private static final String[] FILE_EXTENSIONS = new String[] {FILE_FORMAT};

    private static final String DEFAULT_WINDOWS_OS_NAME_SPACE =
            "C:\\ProgramData\\seatunnel\\checkpoint\\";

    private static final String DEFAULT_LINUX_OS_NAME_SPACE = "/tmp/seatunnel/checkpoint/";

    private static final String META_TMP_SUFFIX = ".tmp";
    private static final String PAYLOAD_TMP_SUFFIX = ".tmp";

    public LocalFileStorage(Map<String, String> configuration) {
        initStorage(configuration);
    }

    @Override
    public void initStorage(Map<String, String> configuration) {
        if (MapUtils.isEmpty(configuration)) {
            setDefaultStorageSpaceByOSName();
            return;
        }
        if (StringUtils.isNotBlank(configuration.get(STORAGE_NAME_SPACE))) {
            setStorageNameSpace(configuration.get(STORAGE_NAME_SPACE));
        }
    }

    /** set default storage root directory */
    private void setDefaultStorageSpaceByOSName() {
        if (System.getProperty("os.name").toLowerCase().contains("windows")) {
            setStorageNameSpace(DEFAULT_WINDOWS_OS_NAME_SPACE);
        } else {
            setStorageNameSpace(DEFAULT_LINUX_OS_NAME_SPACE);
        }
    }

    @Override
    public String storeCheckPoint(PipelineState state) throws CheckpointStorageException {
        byte[] datas;
        try {
            datas = serializeCheckPointData(state);
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to serialize checkpoint data", e);
        }
        // Consider file paths for different operating systems
        String fileName =
                getStorageParentDirectory()
                        + state.getJobId()
                        + File.separator
                        + getCheckPointName(state);

        File file = new File(fileName);
        try {
            FileUtils.touch(file);
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to create checkpoint file " + fileName, e);
        }

        try {
            FileUtils.writeByteArrayToFile(file, datas);
        } catch (IOException e) {
            throw new CheckpointStorageException(
                    "Failed to write checkpoint data to file " + fileName, e);
        }

        return fileName;
    }

    @Override
    public List<PipelineState> getAllCheckpoints(String jobId) throws CheckpointStorageException {
        File filePath = new File(getStorageParentDirectory() + jobId);
        if (!filePath.exists()) {
            return new ArrayList<>();
        }

        Collection<File> fileList;
        try {
            fileList = FileUtils.listFiles(filePath, FILE_EXTENSIONS, true);
        } catch (Exception e) {
            throw new CheckpointStorageException(
                    "Failed to get all checkpoints for job " + jobId, e);
        }
        if (fileList.isEmpty()) {
            log.info("No checkpoint found for this job, the job id is: " + jobId);
            return new ArrayList<>();
        }
        List<PipelineState> states = new ArrayList<>();
        fileList.forEach(
                file -> {
                    try {
                        byte[] data = FileUtils.readFileToByteArray(file);
                        states.add(deserializeCheckPointData(data));
                    } catch (IOException e) {
                        log.error(
                                "Failed to read checkpoint data from file "
                                        + file.getAbsolutePath(),
                                e);
                    }
                });
        return states;
    }

    @Override
    public List<PipelineState> getLatestCheckpoint(String jobId) throws CheckpointStorageException {
        String parentPath = getStorageParentDirectory() + jobId;
        Collection<File> fileList = new ArrayList<>();
        try {
            fileList = FileUtils.listFiles(new File(parentPath), FILE_EXTENSIONS, false);
        } catch (Exception e) {
            if (!(e.getCause() instanceof NoSuchFileException)) {
                throw new CheckpointStorageException(ExceptionUtils.getMessage(e));
            }
        }
        if (fileList.isEmpty()) {
            log.info("No checkpoint found for this  job, the job id is: " + jobId);
            return new ArrayList<>();
        }
        Map<String, File> fileMap =
                fileList.stream()
                        .collect(
                                Collectors.toMap(
                                        File::getName, Function.identity(), (v1, v2) -> v2));
        Set<String> latestPipelines = getLatestPipelineNames(fileMap.keySet());
        List<PipelineState> latestPipelineFiles = new ArrayList<>(latestPipelines.size());
        latestPipelines.forEach(
                fileName -> {
                    File file = fileMap.get(fileName);
                    try {
                        byte[] data = FileUtils.readFileToByteArray(file);
                        latestPipelineFiles.add(deserializeCheckPointData(data));
                    } catch (IOException e) {
                        log.error(
                                "Failed to read checkpoint data from file "
                                        + file.getAbsolutePath(),
                                e);
                    }
                });
        if (latestPipelineFiles.isEmpty()) {
            log.info("No checkpoint found for this job,  the job id:{} " + jobId);
        }
        return latestPipelineFiles;
    }

    @Override
    public PipelineState getLatestCheckpointByJobIdAndPipelineId(String jobId, String pipelineId)
            throws CheckpointStorageException {

        String parentPath = getStorageParentDirectory() + jobId;
        Collection<File> fileList = new ArrayList<>();
        try {
            fileList = FileUtils.listFiles(new File(parentPath), FILE_EXTENSIONS, false);
        } catch (Exception e) {
            if (!(e.getCause() instanceof NoSuchFileException)) {
                throw new CheckpointStorageException(ExceptionUtils.getMessage(e));
            }
        }
        if (fileList.isEmpty()) {
            log.info("No checkpoint found for job, job id is: " + jobId);
            return null;
        }
        List<String> fileNames = fileList.stream().map(File::getName).collect(Collectors.toList());

        String latestFileName =
                getLatestCheckpointFileNameByJobIdAndPipelineId(fileNames, pipelineId);

        AtomicReference<PipelineState> latestFile = new AtomicReference<>(null);
        fileList.forEach(
                file -> {
                    String fileName = file.getName();
                    if (fileName.equals(latestFileName)) {
                        try {
                            byte[] data = FileUtils.readFileToByteArray(file);
                            latestFile.set(deserializeCheckPointData(data));
                        } catch (IOException e) {
                            log.error(
                                    "read checkpoint data from file " + file.getAbsolutePath(), e);
                        }
                    }
                });

        if (latestFile.get() == null) {
            log.info(
                    "No checkpoint found for this job, the job id is: "
                            + jobId
                            + ", pipeline id is: "
                            + pipelineId);
            return null;
        }
        return latestFile.get();
    }

    @Override
    public List<PipelineState> getCheckpointsByJobIdAndPipelineId(String jobId, String pipelineId)
            throws CheckpointStorageException {
        String parentPath = getStorageParentDirectory() + jobId;
        Collection<File> fileList = new ArrayList<>();
        try {
            fileList = FileUtils.listFiles(new File(parentPath), FILE_EXTENSIONS, false);
        } catch (Exception e) {
            if (!(e.getCause() instanceof NoSuchFileException)) {
                throw new CheckpointStorageException(ExceptionUtils.getMessage(e));
            }
        }
        if (fileList.isEmpty()) {
            log.info("No checkpoint found for this job, the job id is: " + jobId);
            return new ArrayList<>();
        }

        List<PipelineState> pipelineStates = new ArrayList<>();
        fileList.forEach(
                file -> {
                    String filePipelineId = getPipelineIdByFileName(file.getName());
                    if (pipelineId.equals(filePipelineId)) {
                        try {
                            byte[] data = FileUtils.readFileToByteArray(file);
                            pipelineStates.add(deserializeCheckPointData(data));
                        } catch (IOException e) {
                            log.error(
                                    "Failed to read checkpoint data from file "
                                            + file.getAbsolutePath(),
                                    e);
                        }
                    }
                });
        return pipelineStates;
    }

    @Override
    public void deleteCheckpoint(String jobId) {
        String jobPath = getStorageParentDirectory() + jobId;
        File file = new File(jobPath);
        try {
            FileUtils.deleteDirectory(file);
        } catch (IOException e) {
            log.warn("Failed to delete checkpoint directory " + jobPath, e);
        }
    }

    @Override
    public PipelineState getCheckpoint(String jobId, String pipelineId, String checkpointId)
            throws CheckpointStorageException {
        String parentPath = getStorageParentDirectory() + jobId;
        Collection<File> fileList = new ArrayList<>();
        try {
            fileList = FileUtils.listFiles(new File(parentPath), FILE_EXTENSIONS, false);
        } catch (Exception e) {
            if (!(e.getCause() instanceof NoSuchFileException)) {
                throw new CheckpointStorageException(ExceptionUtils.getMessage(e));
            }
        }
        if (fileList.isEmpty()) {
            log.info("No checkpoint found for this job,  the job id is: " + jobId);
            return null;
        }
        for (File file : fileList) {
            String fileName = file.getName();
            if (pipelineId.equals(getPipelineIdByFileName(fileName))
                    && checkpointId.equals(getCheckpointIdByFileName(fileName))) {
                try {
                    byte[] data = FileUtils.readFileToByteArray(file);
                    return deserializeCheckPointData(data);
                } catch (Exception e) {
                    log.error(
                            "Failed to delete checkpoint {} for job {}, pipeline {}",
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
        String parentPath = getStorageParentDirectory() + jobId;
        Collection<File> fileList = new ArrayList<>();
        try {
            fileList = FileUtils.listFiles(new File(parentPath), FILE_EXTENSIONS, false);
        } catch (Exception e) {
            if (!(e.getCause() instanceof NoSuchFileException)) {
                throw new CheckpointStorageException(ExceptionUtils.getMessage(e));
            }
        }
        if (fileList.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job " + jobId);
        }
        fileList.forEach(
                file -> {
                    String fileName = file.getName();
                    if (pipelineId.equals(getPipelineIdByFileName(fileName))
                            && checkpointId.equals(getCheckpointIdByFileName(fileName))) {
                        try {
                            FileUtils.delete(file);
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
        String parentPath = getStorageParentDirectory() + jobId;
        Collection<File> fileList = new ArrayList<>();
        try {
            fileList = FileUtils.listFiles(new File(parentPath), FILE_EXTENSIONS, false);
        } catch (Exception e) {
            if (!(e.getCause() instanceof NoSuchFileException)) {
                throw new CheckpointStorageException(ExceptionUtils.getMessage(e));
            }
        }
        if (fileList.isEmpty()) {
            throw new CheckpointStorageException(
                    "No checkpoint found for job, job id is: " + jobId);
        }
        fileList.forEach(
                file -> {
                    String fileName = file.getName();
                    String checkpointIdByFileName = getCheckpointIdByFileName(fileName);
                    if (pipelineId.equals(getPipelineIdByFileName(fileName))
                            && checkpointIdList.contains(checkpointIdByFileName)) {
                        try {
                            FileUtils.delete(file);
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

    // ------------------------------------------------------------------------
    // SavepointStorage capability
    // ------------------------------------------------------------------------

    @Override
    public SavepointWriter beginSavepoint(SavepointRequest request) {
        return new LocalSavepointWriter(request);
    }

    @Override
    public List<SavepointHandle> listCompletedSavepoints(String jobId)
            throws CheckpointStorageException {
        File jobDir = savepointJobDirectory(jobId);
        if (!jobDir.exists()) {
            return new ArrayList<>();
        }
        File[] candidates = jobDir.listFiles(File::isDirectory);
        if (candidates == null) {
            return new ArrayList<>();
        }
        List<SavepointHandle> handles = new ArrayList<>();
        for (File dir : candidates) {
            if (STAGING_DIR.equals(dir.getName())) {
                continue;
            }
            File metaFile = new File(dir, META_FILE_NAME);
            if (!metaFile.exists()) {
                continue;
            }
            try {
                SavepointMeta meta =
                        SavepointStorageUtils.deserializeMeta(
                                FileUtils.readFileToByteArray(metaFile));
                SavepointStorageUtils.verifyManifestChecksum(meta);
                if (!meta.getSavepointId().equals(dir.getName())) {
                    continue;
                }
                if (!manifestFilesComplete(dir, meta)) {
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
                log.warn("Skip unreadable savepoint bundle {}", dir.getAbsolutePath(), e);
            }
        }
        handles.sort(
                Comparator.comparingLong(SavepointHandle::getTriggerTimestamp)
                        .reversed()
                        .thenComparing(SavepointHandle::getSavepointId, Comparator.reverseOrder()));
        return handles;
    }

    @Override
    public SavepointData readSavepoint(String jobId, String savepointId)
            throws CheckpointStorageException {
        File dir = savepointDirectory(jobId, savepointId);
        File metaFile = new File(dir, META_FILE_NAME);
        if (!metaFile.exists()) {
            throw new CheckpointStorageException(
                    "Savepoint not found: job " + jobId + ", savepoint " + savepointId);
        }
        SavepointMeta meta;
        try {
            meta = SavepointStorageUtils.deserializeMeta(FileUtils.readFileToByteArray(metaFile));
        } catch (IOException e) {
            throw new CheckpointStorageException(
                    "Failed to read savepoint metadata for " + savepointId, e);
        }
        SavepointStorageUtils.verifyManifestChecksum(meta);
        SavepointManifestValidator.validateMetadata(meta, jobId, savepointId);
        Map<Integer, PipelineState> pipelineStates = new HashMap<>();
        for (SavepointManifestEntry entry : meta.getPipelines()) {
            File payload = new File(dir, entry.getPayloadFile());
            byte[] bytes;
            try {
                bytes = FileUtils.readFileToByteArray(payload);
            } catch (IOException e) {
                throw new CheckpointStorageException(
                        "Savepoint payload missing for job "
                                + jobId
                                + ", savepoint "
                                + savepointId
                                + ", pipeline "
                                + entry.getPipelineId()
                                + ", file "
                                + entry.getPayloadFile(),
                        e);
            }
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
            PipelineState pipelineState;
            try {
                pipelineState = deserializeCheckPointData(bytes);
            } catch (IOException e) {
                throw new CheckpointStorageException(
                        "Failed to deserialize savepoint payload for job "
                                + jobId
                                + ", savepoint "
                                + savepointId
                                + ", pipeline "
                                + entry.getPipelineId(),
                        e);
            }
            pipelineStates.put(entry.getPipelineId(), pipelineState);
        }
        SavepointManifestValidator.validate(meta, jobId, savepointId, pipelineStates);
        return new SavepointData(jobId, savepointId, meta, pipelineStates);
    }

    @Override
    public void deleteSavepoint(String jobId, String savepointId) {
        File dir = savepointDirectory(jobId, savepointId);
        if (dir.exists()) {
            FileUtils.deleteQuietly(dir);
        }
    }

    @Override
    public void deleteSavepoints(String jobId) {
        File jobDir = savepointJobDirectory(jobId);
        if (jobDir.exists()) {
            FileUtils.deleteQuietly(jobDir);
        }
    }

    private File savepointRoot() {
        return new File(getStorageParentDirectory(), SAVEPOINT_ROOT_DIR);
    }

    private File savepointJobDirectory(String jobId) {
        return new File(savepointRoot(), jobId);
    }

    private File savepointDirectory(String jobId, String savepointId) {
        return new File(savepointJobDirectory(jobId), savepointId);
    }

    private boolean manifestFilesComplete(File dir, SavepointMeta meta) {
        for (SavepointManifestEntry entry : meta.getPipelines()) {
            File payload = new File(dir, entry.getPayloadFile());
            if (!payload.exists() || payload.length() != entry.getPayloadLength()) {
                return false;
            }
        }
        return true;
    }

    /** Savepoint writer implemented on top of the local file system. */
    private final class LocalSavepointWriter implements SavepointWriter {

        private final SavepointRequest request;
        private final File stagingDir;
        /** File name -> serialized payload bytes written to staging. */
        private final Map<String, byte[]> written = new LinkedHashMap<>();

        private final Map<String, Integer> pipelineIds = new HashMap<>();
        private final Map<String, Long> checkpointIds = new HashMap<>();

        LocalSavepointWriter(SavepointRequest request) {
            this.request = request;
            this.stagingDir =
                    new File(
                            new File(new File(savepointRoot(), request.getJobId()), STAGING_DIR),
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
            try {
                Files.createDirectories(stagingDir.toPath());
            } catch (IOException e) {
                throw new CheckpointStorageException(
                        "Failed to create staging directory " + stagingDir, e);
            }
            String fileName =
                    SavepointStorageUtils.pipelinePayloadFileName(
                            state.getPipelineId(), state.getCheckpointId());
            File tmp = new File(stagingDir, fileName + PAYLOAD_TMP_SUFFIX);
            File out = new File(stagingDir, fileName);
            try {
                Files.write(tmp.toPath(), data);
                Files.move(tmp.toPath(), out.toPath(), StandardCopyOption.REPLACE_EXISTING);
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
            File finalDir = savepointDirectory(request.getJobId(), request.getSavepointId());
            if (new File(finalDir, META_FILE_NAME).exists()) {
                throw new CheckpointStorageException(
                        "Savepoint already exists: job "
                                + request.getJobId()
                                + ", savepoint "
                                + request.getSavepointId());
            }
            List<SavepointManifestEntry> entries = new ArrayList<>();
            written.forEach(
                    (fileName, data) -> {
                        entries.add(
                                new SavepointManifestEntry(
                                        pipelineIds.get(fileName),
                                        checkpointIds.get(fileName),
                                        fileName,
                                        data.length,
                                        SavepointStorageUtils.sha256Hex(data),
                                        PAYLOAD_FORMAT_V1));
                    });
            meta.setPipelines(entries);
            SavepointStorageUtils.verifyManifestComplete(request.getSavepointId(), entries);
            meta.setManifestChecksum(SavepointStorageUtils.manifestChecksum(entries));
            try {
                Files.createDirectories(finalDir.toPath());
                File[] staged = stagingDir.listFiles();
                if (staged != null) {
                    for (File file : staged) {
                        if (file.getName().endsWith(PAYLOAD_TMP_SUFFIX)) {
                            continue;
                        }
                        Files.move(
                                file.toPath(),
                                new File(finalDir, file.getName()).toPath(),
                                StandardCopyOption.REPLACE_EXISTING);
                    }
                }
                File metaTmp = new File(finalDir, META_FILE_NAME + META_TMP_SUFFIX);
                Files.write(metaTmp.toPath(), SavepointStorageUtils.serializeMeta(meta));
                Files.move(
                        metaTmp.toPath(),
                        new File(finalDir, META_FILE_NAME).toPath(),
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (IOException e) {
                throw new CheckpointStorageException(
                        "Failed to commit savepoint " + request.getSavepointId(), e);
            }
            try {
                FileUtils.deleteDirectory(stagingDir);
            } catch (IOException e) {
                log.warn("Failed to clean staging dir {}", stagingDir, e);
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
                if (stagingDir.exists()) {
                    FileUtils.deleteDirectory(stagingDir);
                }
            } catch (IOException e) {
                log.warn("Failed to abort savepoint staging {}", stagingDir, e);
            }
        }
    }
}
