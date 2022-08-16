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

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.AbstractCheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class LocalFileStorage extends AbstractCheckpointStorage {

    private static final String[] FILE_EXTENSIONS = new String[]{FILE_FORMAT};

    public LocalFileStorage() {
        // Nothing to do
    }

    @Override
    public void initStorage(Map<String, String> configuration) throws CheckpointStorageException {
        // Nothing to do
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
        String fileName = getStorageParentDirectory() + state.getJobId() + "/" + getCheckPointName(state);

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
        Collection<File> fileList;
        try {
            fileList = FileUtils.listFiles(new File(getStorageParentDirectory() + jobId), FILE_EXTENSIONS, true);
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
        Map<String, File> latestPipelineMap = new HashMap<>();
        fileList.forEach(file -> {
            String filePipelineId = file.getName().split(FILE_NAME_SPLIT)[FILE_NAME_PIPELINE_ID_INDEX];
            int fileVersion = Integer.parseInt(file.getName().split(FILE_NAME_SPLIT)[FILE_SORT_ID_INDEX]);
            if (latestPipelineMap.containsKey(filePipelineId)) {
                int oldVersion = Integer.parseInt(latestPipelineMap.get(filePipelineId).getName().split(FILE_NAME_SPLIT)[FILE_SORT_ID_INDEX]);
                if (fileVersion > oldVersion) {
                    latestPipelineMap.put(filePipelineId, file);
                }
            } else {
                latestPipelineMap.put(filePipelineId, file);
            }
        });
        List<PipelineState> latestPipelineFiles = new ArrayList<>(latestPipelineMap.size());
        latestPipelineMap.values().forEach(file -> {
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

        Collection<File> fileList = FileUtils.listFiles(new File(getStorageParentDirectory() + jobId), FILE_EXTENSIONS, false);
        if (fileList.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job " + jobId);
        }

        AtomicReference<File> latestFile = new AtomicReference<>();
        AtomicInteger latestVersion = new AtomicInteger();
        fileList.forEach(file -> {
            int fileVersion = Integer.parseInt(file.getName().split(FILE_NAME_SPLIT)[FILE_SORT_ID_INDEX]);
            String filePipelineId = file.getName().split(FILE_NAME_SPLIT)[FILE_NAME_PIPELINE_ID_INDEX];
            if (pipelineId.equals(filePipelineId) && fileVersion > latestVersion.get()) {
                latestVersion.set(fileVersion);
                latestFile.set(file);
            }
        });

        if (latestFile.get().exists()) {
            try {
                byte[] data = FileUtils.readFileToByteArray(latestFile.get());
                return deserializeCheckPointData(data);
            } catch (IOException e) {
                throw new CheckpointStorageException("Failed to read checkpoint data from file " + latestFile.get().getAbsolutePath(), e);
            }

        }
        return null;
    }

    @Override
    public List<PipelineState> getCheckpointsByJobIdAndPipelineId(String jobId, String pipelineId) throws CheckpointStorageException {
        Collection<File> fileList = FileUtils.listFiles(new File(getStorageParentDirectory() + jobId), FILE_EXTENSIONS, false);
        if (fileList.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job " + jobId);
        }
        List<PipelineState> pipelineStates = new ArrayList<>();
        fileList.forEach(file -> {
            String filePipelineId = file.getName().split(FILE_NAME_SPLIT)[FILE_NAME_PIPELINE_ID_INDEX];
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

}
