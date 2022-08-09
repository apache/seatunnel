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
import org.apache.seatunnel.engine.checkpoint.storage.api.AbstractCheckPointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckPointStorageException;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public class LocalFileStorage extends AbstractCheckPointStorage {

    private static final String[] FILE_EXTENSIONS = new String[]{FILE_FORMAT};

    public LocalFileStorage(Map<String, String> configuration) {
        // Nothing to do
    }

    @Override
    public void initStorage(Map<String, String> configuration) throws CheckPointStorageException {
        // Nothing to do
        File file = new File(getStorageParentDirectory());
        if (file.exists()) {
            return;
        }
        if (!file.mkdirs()) {
            throw new CheckPointStorageException("Failed to create check-point directory " + getStorageParentDirectory());
        }
    }

    @Override
    public String storeCheckPoint(PipelineState state) throws CheckPointStorageException {
        byte[] datas;
        try {
            datas = serializeCheckPointData(state);
        } catch (IOException e) {
            throw new CheckPointStorageException("Failed to serialize checkpoint data", e);
        }
        //Consider file paths for different operating systems
        String fileName = getStorageParentDirectory() + state.getJobId() + "/" + getCheckPointName(state);

        File file = new File(fileName);
        try {
            FileUtils.touch(file);
        } catch (IOException e) {
            throw new CheckPointStorageException("Failed to create checkpoint file " + fileName, e);
        }

        try {
            FileUtils.writeByteArrayToFile(file, datas);
        } catch (IOException e) {
            throw new CheckPointStorageException("Failed to write checkpoint data to file " + fileName, e);
        }

        return fileName;
    }

    @Override
    public List<PipelineState> getAllCheckpoints(String jobId) {
        Collection<File> fileList = FileUtils.listFiles(new File(getStorageParentDirectory() + jobId), FILE_EXTENSIONS, false);
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
    public PipelineState getLatestCheckpoint(String jobId) throws CheckPointStorageException {
        Collection<File> fileList = FileUtils.listFiles(new File(getStorageParentDirectory() + jobId), FILE_EXTENSIONS, false);
        if (fileList.isEmpty()) {
            throw new CheckPointStorageException("No checkpoint found for job " + jobId);
        }
        AtomicReference<PipelineState> pipelineState = new AtomicReference();
        fileList.stream().max(Comparator.comparing(File::getAbsolutePath)).ifPresent(file -> {
            try {
                byte[] data = FileUtils.readFileToByteArray(file);
                pipelineState.set(deserializeCheckPointData(data));
            } catch (IOException ignored) {
                log.error("Failed to read checkpoint data from file " + file.getAbsolutePath(), ignored);
            }
        });
        if (null != pipelineState.get()) {
            return pipelineState.get();
        }
        throw new CheckPointStorageException("No checkpoint found for job " + jobId);

    }

    @Override
    public PipelineState getCheckpointByJobIdAndPipelineId(String jobId, String pipelineId) throws CheckPointStorageException {

        Collection<File> fileList = FileUtils.listFiles(new File(getStorageParentDirectory() + jobId), FILE_EXTENSIONS, false);
        if (fileList.isEmpty()) {
            throw new CheckPointStorageException("No checkpoint found for job " + jobId);
        }
        AtomicReference<PipelineState> pipelineState = new AtomicReference<>();
        fileList.forEach(file -> {
            String filePipelineId = file.getName().split(FILE_NAME_SPLIT)[FILE_NAME_PIPELINE_ID_INDEX];
            if (pipelineId.equals(filePipelineId)) {
                try {
                    byte[] data = FileUtils.readFileToByteArray(file);
                    pipelineState.set(deserializeCheckPointData(data));
                } catch (IOException e) {
                    log.error("Failed to read checkpoint data from file " + file.getAbsolutePath(), e);
                }
            }
        });

        if (null != pipelineState.get()) {
            return pipelineState.get();
        }
        throw new CheckPointStorageException(String.format("Not found checkpoint data from job id %s and pipeline id %s ", jobId, pipelineId));
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
