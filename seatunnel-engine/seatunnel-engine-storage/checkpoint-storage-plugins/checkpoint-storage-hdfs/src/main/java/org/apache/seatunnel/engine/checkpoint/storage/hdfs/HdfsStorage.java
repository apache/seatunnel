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
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import static org.apache.seatunnel.engine.checkpoint.storage.constants.StorageConstants.STORAGE_NAME_SPACE;

@Slf4j
public class HdfsStorage extends AbstractCheckpointStorage {


    public FileSystem fs;
    private static final String STORAGE_TMP_SUFFIX = "tmp";
    private static final String STORAGE_TYPE_KEY = "storage.type";

    private final Map<String, String> initConfiguration; // 初始配置
    private final ReentrantReadWriteLock fsLock = new ReentrantReadWriteLock(); // 读写锁保护 fs
    private Integer ticketLifetime = 900; //kerberos票据过期时间

    public HdfsStorage(Map<String, String> configuration) throws CheckpointStorageException {
        // 深拷贝初始配置
        this.initConfiguration = deepCopy(configuration);
        this.ticketLifetime=Integer.valueOf(StringUtils.defaultString(configuration.get("seatunnel.hadoop.dfs.kerberos.ticket.lifetime"),"900"));
        // 初次初始化
        initStorage(configuration);
        // 启动周期刷新任务
        startPeriodicInit();
    }

    /**
     * 初始化 FileSystem
     */
    public void initStorage(Map<String, String> configuration) throws CheckpointStorageException {
        Map<String, String> configurationCopy = deepCopy(configuration); // 深拷贝配置

        if (StringUtils.isNotBlank(configurationCopy.get(STORAGE_NAME_SPACE))) {
            setStorageNameSpace(configurationCopy.get(STORAGE_NAME_SPACE));
            configurationCopy.remove(STORAGE_NAME_SPACE);
        }

        Configuration hadoopConf = getConfiguration(configurationCopy);

        fsLock.writeLock().lock(); // 写锁：阻塞所有正在用 fs 的读线程
        try {
            FileSystem newFs = FileSystem.get(hadoopConf);
            if (fs != null) {
                try {
                    fs.close();
                    log.info("Old FileSystem closed.");
                } catch (IOException e) {
                    log.warn("Old FileSystem close failed", e);
                }
            }
            fs = newFs;
            log.info("FileSystem refreshed.");
        } catch (IOException e) {
            throw new CheckpointStorageException("Failed to get file system", e);
        } finally {
            fsLock.writeLock().unlock();
        }
    }
    /**
     * 深拷贝 Map
     */
    private Map<String, String> deepCopy(Map<String, String> source) {
        return new HashMap<>(source);
    }
    /**
     * 启动周期性刷新 FileSystem
     */
    private void startPeriodicInit() {
        ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
        Integer delay = ticketLifetime / 2;//一个过期周期刷新2次
        scheduler.scheduleWithFixedDelay(() -> {
            try {
                initStorage(initConfiguration);
                System.out.println("[HdfsStorage] Periodic fs refresh success.");
            } catch (Exception e) {
                System.err.println("[HdfsStorage] Periodic fs refresh failed: " + e.getMessage());
                e.printStackTrace();
            }
        }, delay, delay, TimeUnit.SECONDS);
        log.info("[HdfsStorage] Periodic fs refresh scheduled. refresh time delay ="+delay);
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
        fsLock.readLock().lock();
        try {
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
        } finally {
            fsLock.readLock().unlock();
        }
    }

    @Override
    public List<PipelineState> getAllCheckpoints(String jobId) throws CheckpointStorageException {
        fsLock.readLock().lock();
        try {
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
        } finally {
            fsLock.readLock().unlock();
        }
    }

    @Override
    public List<PipelineState> getLatestCheckpoint(String jobId) throws CheckpointStorageException {
        fsLock.readLock().lock();
        try {
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
        } finally {
            fsLock.readLock().unlock();
        }

    }

    @Override
    public PipelineState getLatestCheckpointByJobIdAndPipelineId(String jobId, String pipelineId)
            throws CheckpointStorageException {
        fsLock.readLock().lock();
        try {
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
        } finally {
            fsLock.readLock().unlock();
        }

    }

    @Override
    public List<PipelineState> getCheckpointsByJobIdAndPipelineId(String jobId, String pipelineId)
            throws CheckpointStorageException {
        fsLock.readLock().lock();
        try {
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
        } finally {
            fsLock.readLock().unlock();
        }


    }

    @Override
    public void deleteCheckpoint(String jobId) {
        fsLock.readLock().lock();
        try {
            String jobPath = getStorageParentDirectory() + jobId;
            try {
                fs.delete(new Path(jobPath), true);
            } catch (IOException e) {
                log.warn("Failed to delete checkpoint for job {}", jobId, e);
            }
        } finally {
            fsLock.readLock().unlock();
        }

    }

    @Override
    public PipelineState getCheckpoint(String jobId, String pipelineId, String checkpointId)
            throws CheckpointStorageException {
        fsLock.readLock().lock();
        try {

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
        } finally {
            fsLock.readLock().unlock();
        }

    }

    @Override
    public synchronized void deleteCheckpoint(String jobId, String pipelineId, String checkpointId)
            throws CheckpointStorageException {
        fsLock.readLock().lock();
        try {
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
        } finally {
            fsLock.readLock().unlock();
        }


    }

    @Override
    public void deleteCheckpoint(String jobId, String pipelineId, List<String> checkpointIdList)
            throws CheckpointStorageException {
        fsLock.readLock().lock();
        try {
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
        } finally {
            fsLock.readLock().unlock();
        }


    }

    public List<String> getFileNames(String path) throws CheckpointStorageException {
        fsLock.readLock().lock();
        try {
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
        } finally {
            fsLock.readLock().unlock();
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
        fsLock.readLock().lock();
        try {
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
        } finally {
            fsLock.readLock().unlock();
        }


    }
}
