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

import static org.apache.seatunnel.engine.checkpoint.storage.constants.StorageConstants.STORAGE_NAME_SPACE;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.AbstractCheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class LocalFileStorage extends AbstractCheckpointStorage {

    private static final String[] FILE_EXTENSIONS = new String[]{FILE_FORMAT};

    private static final String DEFAULT_WINDOWS_OS_NAME_SPACE = "C:\\ProgramData\\seatunnel\\checkpoint\\";

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

    /**
     * set default storage root directory
     */
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
        //Consider file paths for different operating systems
        String fileName = getStorageParentDirectory() + state.getJobId() + File.separator + getCheckPointName(state);

        File file = new File(fileName);
        try {
            FileUtils.touch(file);
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to create checkpoint file " + fileName, e);
        }

        try {
            FileUtils.writeByteArrayToFile(file, datas);
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to write checkpoint data to file " + fileName, e);
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
            throw new CheckpointStorageException("Failed to get all checkpoints for job " + jobId, e);
        }
        if (fileList.isEmpty()) {
            return new ArrayList<>();
        }
        List<PipelineState> states = new ArrayList<>();
        fileList.forEach(file -> {
            try {
                byte[] data = FileUtils.readFileToByteArray(file);
                states.add(deserializeCheckPointData(data));
            } catch (IOException e) {
                log.error("Failed to read checkpoint data from file " + file.getAbsolutePath(), e);
            }
        });
        return states;
    }

    @Override
    public List<PipelineState> getLatestCheckpoint(String jobId) throws CheckpointStorageException {
        Collection<File> fileList = FileUtils.listFiles(new File(getStorageParentDirectory() + jobId), FILE_EXTENSIONS, false);
        if (fileList.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job " + jobId);
        }
        Map<String, File> fileMap = fileList.stream().collect(Collectors.toMap(File::getName, Function.identity(), (v1, v2) -> v2));
        Set<String> latestPipelines = getLatestPipelineNames(fileMap.keySet());
        List<PipelineState> latestPipelineFiles = new ArrayList<>(latestPipelines.size());
        latestPipelines.forEach(fileName -> {
            File file = fileMap.get(fileName);
            try {
                byte[] data = FileUtils.readFileToByteArray(file);
                latestPipelineFiles.add(deserializeCheckPointData(data));
            } catch (IOException e) {
                log.error("Failed to read checkpoint data from file " + file.getAbsolutePath(), e);
            }
        });
        if (latestPipelineFiles.isEmpty()) {
            throw new CheckpointStorageException("Failed to read checkpoint data from file");
        }
        return latestPipelineFiles;
    }

    @Override
    public PipelineState getLatestCheckpointByJobIdAndPipelineId(String jobId, String pipelineId) throws CheckpointStorageException {

        String parentPath = getStorageParentDirectory() + jobId;
        Collection<File> fileList = FileUtils.listFiles(new File(parentPath), FILE_EXTENSIONS, false);
        if (fileList.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job " + jobId);
        }
        List<String> fileNames = fileList.stream().map(File::getName).collect(Collectors.toList());

        String latestFileName = getLatestCheckpointFileNameByJobIdAndPipelineId(fileNames, pipelineId);

        AtomicReference<PipelineState> latestFile = new AtomicReference<>(null);
        fileList.forEach(file -> {
            String fileName = file.getName();
            if (fileName.equals(latestFileName)) {
                try {
                    byte[] data = FileUtils.readFileToByteArray(file);
                    latestFile.set(deserializeCheckPointData(data));
                } catch (IOException e) {
                    log.error("read checkpoint data from file " + file.getAbsolutePath(), e);
                }
            }
        });

        if (latestFile.get() == null) {
            throw new CheckpointStorageException("Failed to read checkpoint data from file, file name " + latestFileName);
        }
        return latestFile.get();
    }

    @Override
    public List<PipelineState> getCheckpointsByJobIdAndPipelineId(String jobId, String pipelineId) throws CheckpointStorageException {
        Collection<File> fileList = FileUtils.listFiles(new File(getStorageParentDirectory() + jobId), FILE_EXTENSIONS, false);
        if (fileList.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job " + jobId);
        }

        List<PipelineState> pipelineStates = new ArrayList<>();
        fileList.forEach(file -> {
            String filePipelineId = getPipelineIdByFileName(file.getName());
            if (pipelineId.equals(filePipelineId)) {
                try {
                    byte[] data = FileUtils.readFileToByteArray(file);
                    pipelineStates.add(deserializeCheckPointData(data));
                } catch (IOException e) {
                    log.error("Failed to read checkpoint data from file " + file.getAbsolutePath(), e);
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
    public PipelineState getCheckpoint(String jobId, String pipelineId, String checkpointId) throws CheckpointStorageException {
        Collection<File> fileList = FileUtils.listFiles(new File(getStorageParentDirectory() + jobId), FILE_EXTENSIONS, false);
        if (fileList.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job " + jobId);
        }
        for (File file : fileList) {
            String fileName = file.getName();
            if (pipelineId.equals(getPipelineIdByFileName(fileName)) &&
                checkpointId.equals(getCheckpointIdByFileName(fileName))) {
                try {
                    byte[] data = FileUtils.readFileToByteArray(file);
                    return deserializeCheckPointData(data);
                } catch (Exception e) {
                    log.error("Failed to delete checkpoint {} for job {}, pipeline {}", checkpointId, jobId, pipelineId, e);
                }
            }
        }
        throw new CheckpointStorageException(String.format("No checkpoint found, job(%s), pipeline(%s), checkpoint(%s)", jobId, pipelineId, checkpointId));
    }

    @Override
    public void deleteCheckpoint(String jobId, String pipelineId, String checkpointId) throws CheckpointStorageException {
        Collection<File> fileList = FileUtils.listFiles(new File(getStorageParentDirectory() + jobId), FILE_EXTENSIONS, false);
        if (fileList.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job " + jobId);
        }
        fileList.forEach(file -> {
            String fileName = file.getName();
            if (pipelineId.equals(getPipelineIdByFileName(fileName)) &&
                checkpointId.equals(getCheckpointIdByFileName(fileName))) {
                try {
                    FileUtils.delete(file);
                } catch (Exception e) {
                    log.error("Failed to delete checkpoint {} for job {}, pipeline {}", checkpointId, jobId, pipelineId, e);
                }
            }
        });
    }

}
