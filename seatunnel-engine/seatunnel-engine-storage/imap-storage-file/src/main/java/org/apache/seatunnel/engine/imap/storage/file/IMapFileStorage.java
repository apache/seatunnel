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
import static org.apache.seatunnel.engine.imap.storage.file.common.OrcConstants.ORC_FILE_ARCHIVE_PATH;

import org.apache.seatunnel.engine.imap.storage.api.IMapStorage;
import org.apache.seatunnel.engine.imap.storage.api.common.ProtoStuffSerializer;
import org.apache.seatunnel.engine.imap.storage.api.common.Serializer;
import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapData;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.common.FileConstants;
import org.apache.seatunnel.engine.imap.storage.file.disruptor.WALDisruptor;
import org.apache.seatunnel.engine.imap.storage.file.disruptor.WALEventType;
import org.apache.seatunnel.engine.imap.storage.file.future.RequestFuture;
import org.apache.seatunnel.engine.imap.storage.file.future.RequestFutureCache;
import org.apache.seatunnel.engine.imap.storage.file.orc.OrcReader;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ClassUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
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

    public static final int DEFAULT_ARCHIVE_WAIT_TIME_MILLISECONDS = 1000 * 60 * 5;

    public static final int DEFAULT_QUERY_LIST_SIZE = 256;

    public static final int DEFAULT_QUERY_DATA_TIMEOUT_MILLISECONDS = 100;

    private Configuration conf;

    @Override
    public void initialize(Map<String, Object> configuration) {
        Configuration hadoopConf = (Configuration) configuration.get("hadoopConf");
        this.conf = hadoopConf;
        if (configuration.containsKey("namespace")) {
            this.namespace = (String) configuration.get("namespace");
        } else {
            this.namespace = FileConstants.DEFAULT_IMAP_NAMESPACE;
        }

        if (configuration.containsKey("businessName")) {
            this.businessName = (String) configuration.get("businessName");
        }
        if (configuration.containsKey("clusterName")) {
            this.clusterName = (String) configuration.get("clusterName");
        }

        this.region = String.valueOf(System.nanoTime());
        JobConf jobConf = new JobConf(hadoopConf);
        this.businessRootPath = namespace + DEFAULT_IMAP_FILE_PATH_SPLIT + clusterName + DEFAULT_IMAP_FILE_PATH_SPLIT + businessName + DEFAULT_IMAP_FILE_PATH_SPLIT;
        try {
            fs = FileSystem.get(jobConf);
        } catch (IOException e) {
            throw new IMapStorageException("Failed to get file system", e);
        }
        walDisruptor = new WALDisruptor(hadoopConf, businessRootPath + region);
        serializer = new ProtoStuffSerializer();

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
        List<IMapData> imapDataList = new ArrayList<>(DEFAULT_QUERY_LIST_SIZE);
        List<String> fileNames = getFileNames(businessRootPath);
        fileNames.forEach(fileName -> {
            try {
                OrcReader orcReader = new OrcReader(new Path(fileName), conf);
                List<IMapData> dataList = orcReader.queryAll();
                imapDataList.addAll(dataList);
            } catch (IOException e) {
                log.error("read file error, fileName is {}", fileName, e);
            }
        });
        Collections.sort(imapDataList);
        Map<byte[], IMapData> fileDataMaps = new HashMap<>(imapDataList.size());
        Map<byte[], Long> deleteMap = new HashMap<>();
        for (int i = 0; i < imapDataList.size(); i++) {
            IMapData imapData = imapDataList.get(i);
            if (deleteMap.containsKey(imapData.getKey())) {
                continue;
            }
            if (imapData.isDeleted()) {
                deleteMap.put(imapData.getKey(), imapData.getTimestamp());
                continue;
            }
            if (fileDataMaps.containsKey(imapData.getKey())) {
                continue;
            }
            fileDataMaps.put(imapData.getKey(), imapData);
        }
        return parseIMapDataToMap(fileDataMaps);
    }

    @Override
    public List<Object> loadAllKeys() {
        List<IMapData> imapDataList = new ArrayList<>(DEFAULT_QUERY_LIST_SIZE);
        List<String> fileNames = getFileNames(businessRootPath);
        fileNames.forEach(fileName -> {
            try {
                OrcReader orcReader = new OrcReader(new Path(fileName), conf);
                List<IMapData> dataList = orcReader.queryAllKeys();
                imapDataList.addAll(dataList);
            } catch (IOException e) {
                log.error("read file error, fileName is {}", fileName, e);
            }
        });
        Collections.sort(imapDataList);
        Map<byte[], IMapData> fileDataMaps = new HashMap<>(imapDataList.size());
        Map<byte[], Long> deleteMap = new HashMap<>();
        for (IMapData imapData : imapDataList) {
            if (deleteMap.containsKey(imapData.getKey())) {
                continue;
            }
            if (imapData.isDeleted()) {
                deleteMap.put(imapData.getKey(), imapData.getTimestamp());
                continue;
            }
            if (fileDataMaps.containsKey(imapData.getKey())) {
                continue;
            }
            fileDataMaps.put(imapData.getKey(), imapData);
        }
        if (fileDataMaps.isEmpty()) {
            return Collections.emptyList();
        }
        List<Object> result = new ArrayList<>(fileDataMaps.size());

        fileDataMaps.forEach((k, v) -> {
            Object keyData = deserializeData(v.getKey(), v.getValueClassName());
            result.add(keyData);
        });
        return result;
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

    public boolean archive() {
        long requestId = sendToDisruptorQueue(null, WALEventType.IMMEDIATE_ARCHIVE);
        walDisruptor.tryPublish(null, WALEventType.IMMEDIATE_ARCHIVE, requestId);
        return queryExecuteStatus(requestId, DEFAULT_ARCHIVE_WAIT_TIME_MILLISECONDS);
    }

    private IMapFileData parseToIMapFileData(Object key, Object value) throws IOException {
        return IMapFileData.builder()
            .key(serializer.serialize(key))
            .keyClassName(key.getClass().getName())
            .value(serializer.serialize(value))
            .valueClassName(value.getClass().getName())
            .timestamp(System.nanoTime())
            .build();
    }

    private IMapFileData buildDeleteIMapFileData(Object key) throws IOException {
        return IMapFileData.builder()
            .key(serializer.serialize(key))
            .timestamp(System.nanoTime())
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

    private Map<Object, Object> parseIMapDataToMap(Map<byte[], IMapData> fileDataMaps) {
        Map<Object, Object> map = new HashMap<>(fileDataMaps.size());
        fileDataMaps.forEach((key, value) -> {
            Object k = deserializeData(value.getKey(), value.getKeyClassName());
            Object v = deserializeData(value.getValue(), value.getValueClassName());
            map.put(k, v);
        });
        return map;
    }

    private Object deserializeData(byte[] data, String className) {
        try {
            Class<?> clazz = ClassUtils.getClass(className);
            try {
                return serializer.deserialize(data, clazz);
            } catch (IOException e) {
                log.error("deserialize data error", e);
                throw new IMapStorageException("deserialize data error", e);
            }
        } catch (ClassNotFoundException e) {
            log.error("deserialize data error, class name is {}", className, e);
            throw new IMapStorageException(e, "deserialize data error, class name is {}", className);
        }
    }

    private List<String> getFileNames(String path) {
        try {

            RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fs.listFiles(new Path(path), true);
            List<String> fileNames = new ArrayList<>();
            while (fileStatusRemoteIterator.hasNext()) {
                LocatedFileStatus fileStatus = fileStatusRemoteIterator.next();
                if (fileStatus.getPath().getName().startsWith(ORC_FILE_ARCHIVE_PATH)) {
                    fileNames.add(fileStatus.getPath().toString());
                }
            }
            return fileNames;
        } catch (IOException e) {
            throw new IMapStorageException(e, "get file names error,path is s%", path);
        }
    }

}
