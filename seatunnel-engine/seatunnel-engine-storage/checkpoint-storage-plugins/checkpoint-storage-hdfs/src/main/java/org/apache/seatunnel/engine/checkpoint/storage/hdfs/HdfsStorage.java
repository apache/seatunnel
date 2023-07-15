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

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.AbstractCheckpointStorage;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.checkpoint.storage.hdfs.common.AbstractConfiguration;
import org.apache.seatunnel.engine.checkpoint.storage.hdfs.common.FileConfiguration;

import org.apache.commons.lang3.StringUtils;
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
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.seatunnel.engine.checkpoint.storage.constants.StorageConstants.STORAGE_NAME_SPACE;

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

    private Configuration getConfiguration(Map<String, String> config)
            throws CheckpointStorageException {
        String storageType =
                config.getOrDefault(STORAGE_TYPE_KEY, FileConfiguration.LOCAL.toString());
        config.remove(STORAGE_TYPE_KEY);
        AbstractConfiguration configuration =
                FileConfiguration.valueOf(storageType.toUpperCase()).getConfiguration(storageType);
        return configuration.buildConfiguration(config);
    }

    @Override
    public String storeCheckPoint(PipelineState state) throws CheckpointStorageException {
        byte[] datas;
        try {
            datas = serializeCheckPointData(state);
        } catch (IOException e) {
            throw new CheckpointStorageException(
                    "Failed to serialize checkpoint data,state is :" + state, e);
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
                    "Failed to write checkpoint data, state: " + state, e);
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
            log.info("No checkpoint found for this job,  the job id:{} " + jobId);
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

    private List<String> getFileNames(String path) throws CheckpointStorageException {
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
}
