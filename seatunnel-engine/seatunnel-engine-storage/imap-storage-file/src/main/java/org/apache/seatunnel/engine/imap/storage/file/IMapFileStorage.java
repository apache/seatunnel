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

package org.apache.seatunnel.engine.imap.storage.file;

import static org.apache.seatunnel.engine.imap.storage.file.common.FileConstants.DEFAULT_IMAP_FILE_PATH_SPLIT;
import static org.apache.seatunnel.engine.imap.storage.file.common.FileConstants.DEFAULT_IMAP_NAMESPACE;
import static org.apache.seatunnel.engine.imap.storage.file.common.FileConstants.FileInitProperties.BUSINESS_KEY;
import static org.apache.seatunnel.engine.imap.storage.file.common.FileConstants.FileInitProperties.CLUSTER_NAME;
import static org.apache.seatunnel.engine.imap.storage.file.common.FileConstants.FileInitProperties.HDFS_CONFIG_KEY;
import static org.apache.seatunnel.engine.imap.storage.file.common.FileConstants.FileInitProperties.NAMESPACE_KEY;

import org.apache.seatunnel.engine.imap.storage.api.IMapStorage;
import org.apache.seatunnel.engine.imap.storage.api.common.ProtoStuffSerializer;
import org.apache.seatunnel.engine.imap.storage.api.common.Serializer;
import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.FileConstants;
import org.apache.seatunnel.engine.imap.storage.file.common.WALReader;
import org.apache.seatunnel.engine.imap.storage.file.disruptor.WALDisruptor;
import org.apache.seatunnel.engine.imap.storage.file.disruptor.WALEventType;
import org.apache.seatunnel.engine.imap.storage.file.future.RequestFuture;
import org.apache.seatunnel.engine.imap.storage.file.future.RequestFutureCache;

import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * IMapFileStorage
 * Please notice :
 * Only applicable to big data (kv) storage. Otherwise, there may be a lot of fragmented files
 * This is not suitable for frequently updated scenarios because all data is stored as an appended file.
 * There is no guarantee that all files will be up-to-date when a query is made, and this delay depends on the archive cycle.
 * If you write large amounts of data in batches, it is best to archive immediately.
 * Some design detail:
 * base on file, use orc file to store data
 * use disruptor to write data to file
 * use orc reader to read data from file
 * use wal to ensure data consistency
 * use request future to ensure data consistency
 */
@Slf4j
public class IMapFileStorage implements IMapStorage {

    public FileSystem fs;

    public String namespace;

    /**
     * virtual region, Randomly generate a region name
     */
    public String region;

    /**
     * like OSS bucket name
     * It is used to distinguish data storage locations of different business.
     */
    public String businessName;

    /**
     * This parameter is primarily used for cluster isolation
     * we can use this to distinguish different cluster, like cluster1, cluster2
     * and this is also used to distinguish different business
     */
    public String clusterName;

    /**
     * We used disruptor to implement the asynchronous write.
     */
    WALDisruptor walDisruptor;

    /**
     * serializer, default is ProtoStuffSerializer
     */
    Serializer serializer;

    private String businessRootPath = null;

    public static final int DEFAULT_ARCHIVE_WAIT_TIME_MILLISECONDS = 1000 * 60;

    public static final int DEFAULT_QUERY_LIST_SIZE = 256;

    public static final int DEFAULT_QUERY_DATA_TIMEOUT_MILLISECONDS = 100;

    private Configuration conf;

    /**
     * @param configuration configuration
     * @see FileConstants.FileInitProperties
     */
    @Override
    public void initialize(Map<String, Object> configuration) {
        checkInitStorageProperties(configuration);
        Configuration hadoopConf = (Configuration) configuration.get(HDFS_CONFIG_KEY);
        this.conf = hadoopConf;
        this.namespace = (String) configuration.getOrDefault(NAMESPACE_KEY, DEFAULT_IMAP_NAMESPACE);
        this.businessName = (String) configuration.get(BUSINESS_KEY);

        this.clusterName = (String) configuration.get(CLUSTER_NAME);

        this.region = String.valueOf(System.nanoTime());
        this.businessRootPath = namespace + DEFAULT_IMAP_FILE_PATH_SPLIT + clusterName + DEFAULT_IMAP_FILE_PATH_SPLIT + businessName + DEFAULT_IMAP_FILE_PATH_SPLIT;
        try {
            this.fs = FileSystem.get(hadoopConf);
        } catch (IOException e) {
            throw new IMapStorageException("Failed to get file system", e);
        }
        this.serializer = new ProtoStuffSerializer();
        this.walDisruptor = new WALDisruptor(fs, businessRootPath + region + DEFAULT_IMAP_FILE_PATH_SPLIT, serializer);
    }

    @Override
    public boolean store(Object key, Object value) {
        IMapFileData data;
        try {
            data = parseToIMapFileData(key, value);
        } catch (IOException e) {
            log.error("parse to IMapFileData error, key is {}, value is {}", key, value, e);
            return false;
        }

        long requestId = sendToDisruptorQueue(data, WALEventType.APPEND);
        return queryExecuteStatus(requestId);
    }

    @Override
    public Set<Object> storeAll(Map<Object, Object> map) {
        Map<Long, Object> requestMap = new HashMap<>(map.size());
        Set<Object> failures = new HashSet<>();
        map.forEach((key, value) -> {
            try {
                IMapFileData data = parseToIMapFileData(key, value);
                long requestId = sendToDisruptorQueue(data, WALEventType.APPEND);
                requestMap.put(requestId, key);
            } catch (IOException e) {
                log.error("parse to IMapFileData error", e);
                failures.add(key);
            }
        });
        return batchQueryExecuteFailsStatus(requestMap, failures);
    }

    @Override
    public boolean delete(Object key) {
        IMapFileData data;
        try {
            data = buildDeleteIMapFileData(key);
        } catch (IOException e) {
            log.error("parse to IMapFileData error, key is {} ", key, e);
            return false;
        }
        long requestId = sendToDisruptorQueue(data, WALEventType.APPEND);
        return queryExecuteStatus(requestId);
    }

    @Override
    public Set<Object> deleteAll(Collection<Object> keys) {
        Map<Long, Object> requestMap = new HashMap<>(keys.size());
        Set<Object> failures = new HashSet<>();
        keys.forEach(key -> {
            try {
                IMapFileData data = buildDeleteIMapFileData(key);
                long requestId = sendToDisruptorQueue(data, WALEventType.APPEND);
                walDisruptor.tryAppendPublish(data, requestId);
                requestMap.put(requestId, data);
            } catch (IOException e) {
                log.error("parse to IMapFileData error", e);
                failures.add(key);
            }
        });
        return batchQueryExecuteFailsStatus(requestMap, failures);
    }

    @Override
    public Map<Object, Object> loadAll() {
        try {
            WALReader reader = new WALReader(fs, serializer);
            return reader.loadAllData(new Path(businessRootPath), new HashSet<>());
        } catch (IOException e) {
            throw new IMapStorageException("load all data error", e);
        }
    }

    @Override
    public Set<Object> loadAllKeys() {
        try {
            WALReader reader = new WALReader(fs, serializer);
            return reader.loadAllKeys(new Path(businessRootPath));
        } catch (IOException e) {
            throw new IMapStorageException(e, "load all keys error parent path is {}", e, businessRootPath);
        }
    }

    @Override
    public void destroy() {
        log.info("start destroy IMapFileStorage, businessName is {}, cluster name is {}", businessName, region);
        /**
         * 1. close current disruptor
         * 2. delete all  files
         * notice: we can not delete the files in the middle of the write, so some current file may be not deleted
         */
        try {
            walDisruptor.close();
        } catch (IOException e) {
            log.error("close walDisruptor error", e);
        }
        // delete all files
        String parentPath = businessRootPath;

        try {
            fs.delete(new Path(parentPath), true);
        } catch (IOException e) {
            log.error("destroy IMapFileStorage error,businessName is {}, cluster name is {}", businessName, region, e);
        }

    }

    private IMapFileData parseToIMapFileData(Object key, Object value) throws IOException {
        return IMapFileData.builder()
            .key(serializer.serialize(key))
            .keyClassName(key.getClass().getName())
            .value(serializer.serialize(value))
            .valueClassName(value.getClass().getName())
            .timestamp(System.currentTimeMillis())
            .deleted(false)
            .build();
    }

    private IMapFileData buildDeleteIMapFileData(Object key) throws IOException {
        return IMapFileData.builder()
            .key(serializer.serialize(key))
            .keyClassName(key.getClass().getName())
            .timestamp(System.currentTimeMillis())
            .deleted(true)
            .build();
    }

    private long sendToDisruptorQueue(IMapFileData data, WALEventType type) {
        long requestId = RequestFutureCache.getRequestId();
        RequestFuture requestFuture = new RequestFuture();
        RequestFutureCache.put(requestId, requestFuture);
        walDisruptor.tryPublish(data, type, requestId);
        return requestId;
    }

    private boolean queryExecuteStatus(long requestId) {
        return queryExecuteStatus(requestId, DEFAULT_QUERY_DATA_TIMEOUT_MILLISECONDS);
    }

    private boolean queryExecuteStatus(long requestId, long timeout) {
        RequestFuture requestFuture = RequestFutureCache.get(requestId);
        try {
            if (requestFuture.isDone() || Boolean.TRUE.equals(requestFuture.get(timeout, TimeUnit.MILLISECONDS))) {
                return true;
            }
        } catch (Exception e) {
            log.error("wait for write status error", e);
        } finally {
            RequestFutureCache.remove(requestId);
        }
        return false;
    }

    private Set<Object> batchQueryExecuteFailsStatus(Map<Long, Object> requestMap, Set<Object> failures) {
        for (Map.Entry<Long, Object> entry : requestMap.entrySet()) {
            boolean success = false;
            RequestFuture requestFuture = RequestFutureCache.get(entry.getKey());
            try {
                if (requestFuture.isDone() || Boolean.TRUE.equals(requestFuture.get())) {
                    success = true;
                }
            } catch (Exception e) {
                log.error("wait for write status error", e);
            } finally {
                RequestFutureCache.remove(entry.getKey());
            }
            if (!success) {
                failures.add(entry.getValue());
            }
        }
        return failures;
    }

    private void checkInitStorageProperties(Map<String, Object> properties) {
        if (properties == null || properties.isEmpty()) {
            throw new IllegalArgumentException("init file storage properties is empty");
        }
        List<String> requiredProperties = Arrays.asList(BUSINESS_KEY, CLUSTER_NAME, HDFS_CONFIG_KEY);
        for (String requiredProperty : requiredProperties) {
            if (!properties.containsKey(requiredProperty)) {
                throw new IllegalArgumentException("init file storage properties is not contains " + requiredProperty);
            }
        }
    }

}
