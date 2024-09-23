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

package org.apache.seatunnel.engine.checkpoint.storage.api;

import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.common.StorageThreadFactory;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.serializer.api.Serializer;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
public abstract class AbstractCheckpointStorage implements CheckpointStorage {

    /**
     * serializer,default is protostuff,if necessary, consider other serialization methods,
     * temporarily hard-coding
     */
    private final Serializer serializer = new ProtoStuffSerializer();

    public static final String DEFAULT_CHECKPOINT_FILE_PATH_SPLIT = "/";

    /** storage root directory if not set, use default value */
    private String storageNameSpace = "/seatunnel/checkpoint/";

    public static final String FILE_NAME_SPLIT = "-";

    public static final int FILE_NAME_PIPELINE_ID_INDEX = 2;

    public static final int FILE_NAME_CHECKPOINT_ID_INDEX = 3;

    public static final int FILE_SORT_ID_INDEX = 0;

    public static final int FILE_NAME_RANDOM_RANGE = 1000;

    public static final String FILE_FORMAT = "ser";

    private volatile ExecutorService executorService;

    private static final int DEFAULT_THREAD_POOL_MIN_SIZE =
            Runtime.getRuntime().availableProcessors() * 2 + 1;

    private static final int DEFAULT_THREAD_POOL_MAX_SIZE =
            Runtime.getRuntime().availableProcessors() * 4 + 1;

    private static final int DEFAULT_THREAD_POOL_QUENE_SIZE = 1024;

    /**
     * init storage instance
     *
     * @param configuration configuration key: storage root directory value: storage root directory
     * @throws CheckpointStorageException if storage init failed
     */
    public abstract void initStorage(Map<String, String> configuration)
            throws CheckpointStorageException;

    public String getStorageParentDirectory() {
        return storageNameSpace;
    }

    public String getCheckPointName(PipelineState state) {
        return System.currentTimeMillis()
                + FILE_NAME_SPLIT
                + ThreadLocalRandom.current().nextInt(FILE_NAME_RANDOM_RANGE)
                + FILE_NAME_SPLIT
                + state.getPipelineId()
                + FILE_NAME_SPLIT
                + state.getCheckpointId()
                + "."
                + FILE_FORMAT;
    }

    public byte[] serializeCheckPointData(PipelineState state) throws IOException {
        return serializer.serialize(state);
    }

    public PipelineState deserializeCheckPointData(byte[] data) throws IOException {
        return serializer.deserialize(data, PipelineState.class);
    }

    public void setStorageNameSpace(String storageNameSpace) {
        if (storageNameSpace != null) {
            if (!storageNameSpace.endsWith(DEFAULT_CHECKPOINT_FILE_PATH_SPLIT)) {
                storageNameSpace = storageNameSpace + DEFAULT_CHECKPOINT_FILE_PATH_SPLIT;
            }
            this.storageNameSpace = storageNameSpace;
        }
    }

    public Set<String> getLatestPipelineNames(Collection<String> fileNames) {
        Map<String, String> latestPipelineMap = new HashMap<>();
        Map<String, Long> latestPipelineVersionMap = new HashMap<>();
        fileNames.forEach(
                fileName -> {
                    String[] fileNameSegments = getFileNameSegments(fileName);
                    long fileVersion = Long.parseLong(fileNameSegments[FILE_SORT_ID_INDEX]);
                    String filePipelineId = fileNameSegments[FILE_NAME_PIPELINE_ID_INDEX];
                    Long oldVersion = latestPipelineVersionMap.get(filePipelineId);
                    if (Objects.isNull(oldVersion) || fileVersion > oldVersion) {
                        latestPipelineVersionMap.put(filePipelineId, fileVersion);
                        latestPipelineMap.put(filePipelineId, fileName);
                    }
                });
        return latestPipelineMap.entrySet().stream()
                .map(Map.Entry::getValue)
                .collect(Collectors.toSet());
    }

    /**
     * get latest checkpoint file name
     *
     * @param fileNames file names
     * @return latest checkpoint file name
     */
    public String getLatestCheckpointFileNameByJobIdAndPipelineId(
            List<String> fileNames, String pipelineId) {
        AtomicReference<String> latestFileName = new AtomicReference<>();
        AtomicLong latestVersion = new AtomicLong();
        fileNames.forEach(
                fileName -> {
                    String[] fileNameSegments = getFileNameSegments(fileName);
                    long fileVersion = Long.parseLong(fileNameSegments[FILE_SORT_ID_INDEX]);
                    String filePipelineId = fileNameSegments[FILE_NAME_PIPELINE_ID_INDEX];
                    if (pipelineId.equals(filePipelineId) && fileVersion > latestVersion.get()) {
                        latestVersion.set(fileVersion);
                        latestFileName.set(fileName);
                    }
                });
        return latestFileName.get();
    }

    private String[] getFileNameSegments(String fileName) {
        return fileName.split(FILE_NAME_SPLIT);
    }

    /**
     * get the pipeline id of the file name
     *
     * @param fileName file names. note: file name cannot contain parent path
     * @return the pipeline id of the file.
     */
    public String getPipelineIdByFileName(String fileName) {
        return getFileNameSegments(fileName)[FILE_NAME_PIPELINE_ID_INDEX];
    }

    /**
     * get the checkpoint id of the file name
     *
     * @param fileName file names. note: file name cannot contain parent path
     * @return the checkpoint id of the file.
     */
    public String getCheckpointIdByFileName(String fileName) {
        return getFileNameSegments(fileName)[FILE_NAME_CHECKPOINT_ID_INDEX].split("\\.")[0];
    }

    @Override
    public void asyncStoreCheckPoint(PipelineState state) {
        initExecutor();
        this.executorService.submit(
                () -> {
                    try {
                        storeCheckPoint(state);
                    } catch (Throwable e) {
                        log.error(
                                String.format(
                                        "store checkpoint failed, job id : %s, pipeline id : %d",
                                        state.getJobId(), state.getPipelineId()),
                                e);
                    }
                });
    }

    private void initExecutor() {
        if (null == this.executorService || this.executorService.isShutdown()) {
            synchronized (this) {
                if (null == this.executorService || this.executorService.isShutdown()) {
                    this.executorService =
                            new ThreadPoolExecutor(
                                    DEFAULT_THREAD_POOL_MIN_SIZE,
                                    DEFAULT_THREAD_POOL_MAX_SIZE,
                                    0L,
                                    TimeUnit.MILLISECONDS,
                                    new LinkedBlockingQueue<>(DEFAULT_THREAD_POOL_QUENE_SIZE),
                                    new StorageThreadFactory());
                }
            }
        }
    }
}
