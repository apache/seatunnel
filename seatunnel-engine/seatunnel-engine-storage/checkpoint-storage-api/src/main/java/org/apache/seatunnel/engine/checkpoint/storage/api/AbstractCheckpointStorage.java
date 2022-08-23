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
import org.apache.seatunnel.engine.checkpoint.storage.common.ProtoStuffSerializer;
import org.apache.seatunnel.engine.checkpoint.storage.common.Serializer;
import org.apache.seatunnel.engine.checkpoint.storage.common.StorageThreadFactory;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

@Slf4j
public abstract class AbstractCheckpointStorage implements CheckpointStorage {

    /**
     * serializer,default is protostuff,if necessary, consider other serialization methods, temporarily hard-coding
     */
    private final Serializer serializer = new ProtoStuffSerializer();

    public static final String DEFAULT_CHECKPOINT_FILE_PATH_SPLIT = "/";

    /**
     * storage root directory
     * if not set, use default value
     */
    private String storageNameSpace = "/seatunnel/checkpoint/";

    public static final String FILE_NAME_SPLIT = "-";

    public static final int FILE_NAME_PIPELINE_ID_INDEX = 2;

    public static final int FILE_SORT_ID_INDEX = 0;

    public static final int FILE_NAME_RANDOM_RANGE = 1000;

    public static final String FILE_FORMAT = "ser";

    private ExecutorService executorService;

    private static final int DEFAULT_THREAD_POOL_MIN_SIZE = Runtime.getRuntime().availableProcessors() * 2 + 1;

    private static final int DEFAULT_THREAD_POOL_MAX_SIZE = Runtime.getRuntime().availableProcessors() * 4 + 1;

    private static final int DEFAULT_THREAD_POOL_QUENE_SIZE = 1024;

    /**
     * init storage instance
     *
     * @param configuration configuration
     *                      key: storage root directory
     *                      value: storage root directory
     * @throws CheckpointStorageException if storage init failed
     */
    public abstract void initStorage(Map<String, String> configuration) throws CheckpointStorageException;

    public String getStorageParentDirectory() {
        return storageNameSpace;
    }

    public String getCheckPointName(PipelineState state) {
        return System.nanoTime() + FILE_NAME_SPLIT + ThreadLocalRandom.current().nextInt(FILE_NAME_RANDOM_RANGE) + FILE_NAME_SPLIT + state.getPipelineId() + FILE_NAME_SPLIT + state.getCheckpointId() + "." + FILE_FORMAT;
    }

    public byte[] serializeCheckPointData(PipelineState state) throws IOException {
        return serializer.serialize(state);
    }

    public PipelineState deserializeCheckPointData(byte[] data) throws IOException {
        return serializer.deserialize(data, PipelineState.class);
    }

    public void setStorageNameSpace(String storageNameSpace) {
        if (storageNameSpace != null) {
            this.storageNameSpace = storageNameSpace;
        }
    }

    public Set<String> getLatestPipelineNames(List<String> fileNames) {
        Map<String, String> latestPipelineMap = new HashMap<>();
        fileNames.forEach(fileName -> {
            String[] fileNameSegments = fileName.split(FILE_NAME_SPLIT);
            long fileVersion = Long.parseLong(fileNameSegments[FILE_SORT_ID_INDEX]);
            String filePipelineId = fileNameSegments[FILE_NAME_PIPELINE_ID_INDEX];
            if (latestPipelineMap.containsKey(filePipelineId)) {
                long oldVersion = Long.parseLong(latestPipelineMap.get(filePipelineId).split(FILE_NAME_SPLIT)[FILE_SORT_ID_INDEX]);
                if (fileVersion > oldVersion) {
                    latestPipelineMap.put(filePipelineId, fileName);
                }
            } else {
                latestPipelineMap.put(filePipelineId, fileName);
            }
        });
        Set<String> latestPipelines = new HashSet<>(latestPipelineMap.size());
        latestPipelineMap.forEach((pipelineId, fileName) -> latestPipelines.add(fileName));
        return latestPipelines;
    }

    /**
     * get latest checkpoint file name
     *
     * @param fileNames file names
     * @return latest checkpoint file name
     */
    public String getLatestCheckpointFileNameByJobIdAndPipelineId(List<String> fileNames, String pipelineId) {
        AtomicReference<String> latestFileName = new AtomicReference<>();
        AtomicLong latestVersion = new AtomicLong();
        fileNames.forEach(fileName -> {
            String[] fileNameSegments = fileName.split(FILE_NAME_SPLIT);
            long fileVersion = Long.parseLong(fileNameSegments[FILE_SORT_ID_INDEX]);
            String filePipelineId = fileNameSegments[FILE_NAME_PIPELINE_ID_INDEX];
            if (pipelineId.equals(filePipelineId) && fileVersion > latestVersion.get()) {
                latestVersion.set(fileVersion);
                latestFileName.set(fileName);
            }
        });
        return latestFileName.get();
    }

    /**
     * get the latest checkpoint file name
     *
     * @param fileName file names. note: file name cannot contain parent path
     * @return latest checkpoint file name
     */
    public String getPipelineIdByFileName(String fileName) {
        return fileName.split(FILE_NAME_SPLIT)[FILE_NAME_PIPELINE_ID_INDEX];
    }

    @Override
    public void asyncStoreCheckPoint(PipelineState state) {
        initExecutor();
        this.executorService.submit(() -> {
            try {
                storeCheckPoint(state);
            } catch (Exception e) {
                log.error(String.format("store checkpoint failed, job id : %s, pipeline id : %d", state.getJobId(), state.getPipelineId()), e);
            }
        });
    }

    private void initExecutor() {
        if (null == this.executorService || this.executorService.isShutdown()) {
            synchronized (this) {
                if (null == this.executorService || this.executorService.isShutdown()) {
                    this.executorService = new ThreadPoolExecutor(DEFAULT_THREAD_POOL_MIN_SIZE, DEFAULT_THREAD_POOL_MAX_SIZE, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>(DEFAULT_THREAD_POOL_QUENE_SIZE), new StorageThreadFactory());
                }
            }
        }
    }
}
