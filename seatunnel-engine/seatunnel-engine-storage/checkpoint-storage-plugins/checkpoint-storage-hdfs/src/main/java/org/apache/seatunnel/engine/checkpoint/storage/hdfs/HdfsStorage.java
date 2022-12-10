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

import static org.apache.seatunnel.engine.checkpoint.storage.constants.StorageConstants.STORAGE_NAME_SPACE;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.AbstractCheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.hdfs.common.AbstractConfiguration;
import org.apache.seatunnel.engine.checkpoint.storage.hdfs.common.FileConfiguration;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Slf4j
public class HdfsStorage extends AbstractCheckpointStorage {

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

    private Configuration getConfiguration(Map<String, String> config) throws CheckpointStorageException {
        String storageType = config.getOrDefault(STORAGE_TYPE_KEY, FileConfiguration.LOCAL.toString());
        config.remove(STORAGE_TYPE_KEY);
        AbstractConfiguration configuration = FileConfiguration.valueOf(storageType.toUpperCase()).getConfiguration(storageType);
        return configuration.buildConfiguration(config);
    }

    @Override
    public String storeCheckPoint(PipelineState state) throws CheckpointStorageException {
        byte[] datas;
        try {
            datas = serializeCheckPointData(state);
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to serialize checkpoint data,state is :" + state, e);
        }
        Path filePath = new Path(getStorageParentDirectory() + state.getJobId() + "/" + getCheckPointName(state));

        Path tmpFilePath = new Path(getStorageParentDirectory() + state.getJobId() + "/" + getCheckPointName(state) + STORAGE_TMP_SUFFIX);
        try (FSDataOutputStream out = fs.create(tmpFilePath)) {
            out.write(datas);
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to write checkpoint data, state: " + state, e);
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
            throw new CheckpointStorageException("No checkpoint found for job, job id is: " + jobId);
        }
        List<PipelineState> states = new ArrayList<>();
        fileNames.forEach(file -> {
            try {
                states.add(readPipelineState(file, jobId));
            } catch (CheckpointStorageException e) {
                log.error("Failed to read checkpoint data from file: " + file, e);
            }
        });
        if (states.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job, job id is: " + jobId);
        }
        return states;
    }

    @Override
    public List<PipelineState> getLatestCheckpoint(String jobId) throws CheckpointStorageException {
        String path = getStorageParentDirectory() + jobId;
        List<String> fileNames = getFileNames(path);
        if (fileNames.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job, job id is: " + jobId);
        }
        Set<String> latestPipelineNames = getLatestPipelineNames(fileNames);
        List<PipelineState> latestPipelineStates = new ArrayList<>();
        latestPipelineNames.forEach(fileName -> {
            try {
                latestPipelineStates.add(readPipelineState(fileName, jobId));
            } catch (CheckpointStorageException e) {
                log.error("Failed to read pipeline state for file: {}", fileName, e);
            }
        });

        if (latestPipelineStates.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job, job id:{} " + jobId);
        }
        return latestPipelineStates;
    }

    @Override
    public PipelineState getLatestCheckpointByJobIdAndPipelineId(String jobId, String pipelineId) throws CheckpointStorageException {
        String path = getStorageParentDirectory() + jobId;
        List<String> fileNames = getFileNames(path);
        if (fileNames.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job, job id is: " + jobId);
        }

        String latestFileName = getLatestCheckpointFileNameByJobIdAndPipelineId(fileNames, pipelineId);
        if (latestFileName == null) {
            throw new CheckpointStorageException("No checkpoint found for job, job id is: " + jobId + ", pipeline id is: " + pipelineId);
        }
        return readPipelineState(latestFileName, jobId);
    }

    @Override
    public List<PipelineState> getCheckpointsByJobIdAndPipelineId(String jobId, String pipelineId) throws CheckpointStorageException {
        String path = getStorageParentDirectory() + jobId;
        List<String> fileNames = getFileNames(path);
        if (fileNames.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job, job id is: " + jobId);
        }

        List<PipelineState> pipelineStates = new ArrayList<>();
        fileNames.forEach(file -> {
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
            log.error("Failed to delete checkpoint for job {}", jobId, e);
        }
    }

    @Override
    public PipelineState getCheckpoint(String jobId, String pipelineId, String checkpointId) throws CheckpointStorageException {
        String path = getStorageParentDirectory() + jobId;
        List<String> fileNames = getFileNames(path);
        if (fileNames.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job, job id is: " + jobId);
        }
        for (String fileName : fileNames) {
            if (pipelineId.equals(getPipelineIdByFileName(fileName)) &&
                checkpointId.equals(getCheckpointIdByFileName(fileName))) {
                try {
                    return readPipelineState(fileName, jobId);
                } catch (Exception e) {
                    log.error("Failed to get checkpoint {} for job {}, pipeline {}", checkpointId, jobId, pipelineId, e);
                }
            }
        }
        throw new CheckpointStorageException(String.format("No checkpoint found, job(%s), pipeline(%s), checkpoint(%s)", jobId, pipelineId, checkpointId));
    }

    @Override
    public void deleteCheckpoint(String jobId, String pipelineId, String checkpointId) throws CheckpointStorageException {
        String path = getStorageParentDirectory() + jobId;
        List<String> fileNames = getFileNames(path);
        if (fileNames.isEmpty()) {
            throw new CheckpointStorageException("No checkpoint found for job, job id is: " + jobId);
        }
        fileNames.forEach(fileName -> {
            if (pipelineId.equals(getPipelineIdByFileName(fileName)) &&
                checkpointId.equals(getCheckpointIdByFileName(fileName))) {
                try {
                    fs.delete(new Path(fileName), false);
                } catch (Exception e) {
                    log.error("Failed to delete checkpoint {} for job {}, pipeline {}", checkpointId, jobId, pipelineId, e);
                }
            }
        });
    }

    private List<String> getFileNames(String path) throws CheckpointStorageException {
        try {

            RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fs.listFiles(new Path(path), false);
            List<String> fileNames = new ArrayList<>();
            while (fileStatusRemoteIterator.hasNext()) {
                LocatedFileStatus fileStatus = fileStatusRemoteIterator.next();
                if (!fileStatus.getPath().getName().endsWith(FILE_FORMAT)) {
                    fileNames.add(fileStatus.getPath().getName());
                }
                fileNames.add(fileStatus.getPath().getName());
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
    private PipelineState readPipelineState(String fileName, String jobId) throws CheckpointStorageException {
        fileName = getStorageParentDirectory() + jobId + DEFAULT_CHECKPOINT_FILE_PATH_SPLIT + fileName;
        try (FSDataInputStream in = fs.open(new Path(fileName))) {
            byte[] datas = new byte[in.available()];
            in.read(datas);
            return deserializeCheckPointData(datas);
        } catch (IOException e) {
            throw new CheckpointStorageException(String.format("Failed to read checkpoint data, file name is %s,job id is %s", fileName, jobId), e);
        }
    }

}
