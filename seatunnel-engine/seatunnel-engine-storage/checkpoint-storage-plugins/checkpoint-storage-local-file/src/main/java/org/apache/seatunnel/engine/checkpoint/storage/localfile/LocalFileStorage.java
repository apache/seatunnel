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

import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.checkpoint.storage.constants.StorageConstants.STORAGE_NAME_SPACE;

@Slf4j
public class LocalFileStorage extends AbstractCheckpointStorage {

    private static final String[] FILE_EXTENSIONS = new String[] {FILE_FORMAT};

    private static final String DEFAULT_WINDOWS_OS_NAME_SPACE =
            "C:\\ProgramData\\seatunnel\\checkpoint\\";

    private static final String DEFAULT_LINUX_OS_NAME_SPACE = "/tmp/seatunnel/checkpoint/";

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

    PipelineState readRecoveryCheckpoint(Path file) throws IOException {
        return deserializeCheckPointData(Files.readAllBytes(file));
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
        Path directory = Paths.get(getStorageParentDirectory(), jobId);
        NoSuchFileException disappeared = null;
        long highestListedVersion = -1;
        long highestListedCheckpoint = -1;
        for (int attempt = 0; attempt < 3; attempt++) {
            try (DirectoryStream<Path> files = Files.newDirectoryStream(directory)) {
                Path newest = null;
                long newestVersion = -1;
                long newestId = -1;
                for (Path file : files) {
                    String name = file.getFileName().toString();
                    if (name.endsWith("." + FILE_FORMAT)
                            && pipelineId.equals(getPipelineIdByFileName(name))) {
                        long checkpointId = Long.parseLong(getCheckpointIdByFileName(name));
                        long fileVersion =
                                Long.parseLong(name.split(FILE_NAME_SPLIT)[FILE_SORT_ID_INDEX]);
                        if (fileVersion > newestVersion
                                || fileVersion == newestVersion && checkpointId > newestId) {
                            newestVersion = fileVersion;
                            newestId = checkpointId;
                            newest = file;
                        }
                    }
                }
                if (newest == null) {
                    if (disappeared != null) {
                        throw new CheckpointStorageException(
                                "Checkpoint disappeared during recovery", disappeared);
                    }
                    return null;
                }
                if (newestVersion < highestListedVersion
                        || newestVersion == highestListedVersion
                                && newestId < highestListedCheckpoint) {
                    continue;
                }
                highestListedVersion = newestVersion;
                highestListedCheckpoint = newestId;
                PipelineState state = readRecoveryCheckpoint(newest);
                if (!jobId.equals(state.getJobId())
                        || !pipelineId.equals(String.valueOf(state.getPipelineId()))
                        || state.getCheckpointId() != newestId) {
                    throw new CheckpointStorageException(
                            "Checkpoint identity does not match the selected file");
                }
                return state;
            } catch (NoSuchFileException failure) {
                if (directory.toString().equals(failure.getFile()) && disappeared == null) {
                    return null;
                }
                disappeared = failure;
            } catch (IOException | RuntimeException failure) {
                throw new CheckpointStorageException(
                        "Failed to read latest checkpoint for recovery", failure);
            }
        }
        throw new CheckpointStorageException("Checkpoint changed during recovery", disappeared);
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
}
