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

package org.apache.seatunnel.engine.server.rocksdb;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.server.MapStoreConfig;
import org.apache.seatunnel.engine.imap.storage.api.RocksDBStorageFactory;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.persistence.rocksdb.FileMapStoreManager;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import lombok.extern.slf4j.Slf4j;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class RocksDBStateBackend {
    public static final String DB_PATH = "rocksdb";
    public static final String DEFAULT_NAME = "default";

    private final RocksDB db;
    private final DBOptions dbOptions;
    private final List<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>();
    private final List<ColumnFamilyHandle> columnFamilyHandles = new ArrayList<>();
    private final Map<String, ColumnFamilyHandle> columnFamilyMap = new HashMap<>();
    private final Map<String, RocksDBValueState<Object, Object>> valueStateMap = new HashMap<>();
    private final List<String> initialStateNames =
            new ArrayList<>(Arrays.asList(DEFAULT_NAME, Constant.IMAP_RUNNING_JOB_METRICS));

    private FileMapStoreManager fileMapStoreManager;

    public RocksDBStateBackend(
            String dbPath, RocksDBStorageFactory factory, MapStoreConfig mapStoreConfig)
            throws RocksDBException {
        RocksDB.loadLibrary();
        try {
            List<ColumnFamilyDescriptor> descriptors = getColumnFamilyDescriptors(dbPath);

            this.dbOptions =
                    new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
            this.db = RocksDB.open(dbOptions, dbPath, descriptors, this.columnFamilyHandles);

            initializeColumnFamilyMapAndValueStateMap();

            if (mapStoreConfig != null && mapStoreConfig.isMapStoreEnabled()) {
                this.fileMapStoreManager =
                        new FileMapStoreManager(initialStateNames, factory, mapStoreConfig.toMap());
            }
        } catch (RocksDBException e) {
            log.error("Failed to open RocksDB at {}: {}", dbPath, e.getMessage(), e);
            close(dbPath);
            throw e;
        }
    }

    private List<ColumnFamilyDescriptor> getColumnFamilyDescriptors(String dbPath) {
        addExistingColumnFamilies(dbPath);

        List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
        for (String name : initialStateNames) {
            ColumnFamilyOptions options = new ColumnFamilyOptions();
            columnFamilyOptions.add(options);
            if (DEFAULT_NAME.equals(name)) {
                descriptors.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, options));
            } else {
                descriptors.add(
                        new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.UTF_8), options));
            }
        }
        return descriptors;
    }

    private void addExistingColumnFamilies(String dbPath) {
        try (Options options = new Options()) {
            List<byte[]> existing = RocksDB.listColumnFamilies(options, dbPath);

            List<String> existingNames = new ArrayList<>();
            for (byte[] bytes : existing) {
                if (Arrays.equals(bytes, RocksDB.DEFAULT_COLUMN_FAMILY)) {
                    existingNames.add(DEFAULT_NAME);
                } else {
                    existingNames.add(new String(bytes, StandardCharsets.UTF_8));
                }
            }

            for (String name : existingNames) {
                if (!initialStateNames.contains(name)) {
                    this.initialStateNames.add(name);
                }
            }
        } catch (RocksDBException ignored) {
            log.info("RocksDB at {} does not exist. It will be created.", dbPath);
        }
    }

    private void initializeColumnFamilyMapAndValueStateMap() {
        int idx = 0;
        for (String name : initialStateNames) {
            if (StringUtils.isBlank(name)) continue;
            if (idx < columnFamilyHandles.size()) {
                columnFamilyMap.put(name, columnFamilyHandles.get(idx));
            }
            idx++;
        }

        for (String name : initialStateNames) {
            if (StringUtils.isBlank(name)) continue;
            RocksDBValueState<Object, Object> valueState =
                    new RocksDBValueState<>(
                            db, columnFamilyMap.get(name), new ProtoStuffSerializer());
            valueStateMap.put(name, valueState);
        }
    }

    public void init() {
        if (fileMapStoreManager == null) return;
        for (String name : initialStateNames) {
            Map<Object, Object> loaded = fileMapStoreManager.loadAll(name);
            RocksDBValueState<Object, Object> valueState = getValueState(name);
            for (Map.Entry<Object, Object> entry : loaded.entrySet()) {
                valueState.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public <K, V> RocksDBValueState<K, V> getValueState(String stateName) {
        @SuppressWarnings("unchecked")
        RocksDBValueState<K, V> rocksDBValueState =
                (RocksDBValueState<K, V>) valueStateMap.get(stateName);
        if (rocksDBValueState == null) {
            throw CommonError.illegalArgument(stateName, "getRocksDBValueState");
        }
        return rocksDBValueState;
    }

    public <K, V> void put(String stateName, K key, V value) {
        RocksDBValueState<K, V> valueState = getValueState(stateName);
        valueState.compute(key, oldVal -> mergeValues(oldVal, value));
        if (fileMapStoreManager != null) {
            V merged = valueState.get(key);
            fileMapStoreManager.put(stateName, key, merged);
        }
    }

    public <K, V> void putAll(String stateName, Map<K, V> map) {
        final RocksDBValueState<K, V> valueState = getValueState(stateName);
        for (Map.Entry<K, V> e : map.entrySet()) {
            K key = e.getKey();
            valueState.compute(key, oldVal -> mergeValues(oldVal, e.getValue()));
            if (fileMapStoreManager != null) {
                V merged = valueState.get(key);
                fileMapStoreManager.put(stateName, key, merged);
            }
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    private static <V> V mergeValues(V oldVal, V newVal) {
        if (oldVal == null) return newVal;

        if (oldVal instanceof Map && newVal instanceof Map) {
            Map merged = new HashMap((Map) oldVal);
            merged.putAll((Map) newVal);
            return (V) merged;
        }

        return newVal;
    }

    public <K, V> void remove(String stateName, K key) {
        RocksDBValueState<K, V> valueState = getValueState(stateName);
        valueState.remove(key);
        if (fileMapStoreManager != null) fileMapStoreManager.remove(stateName, key);
    }

    public void close(String dbPath) {
        close(dbPath, false);
    }

    public void close(String dbPath, boolean destroyFiles) {
        if (fileMapStoreManager != null) this.fileMapStoreManager.destroy();

        for (State state : valueStateMap.values()) {
            state.close();
        }
        valueStateMap.clear();
        columnFamilyHandles.clear();
        columnFamilyMap.clear();

        try {
            if (db != null) db.close();
        } catch (Exception ignored) {
            log.warn("Failed to close RocksDB", ignored);
        }

        try {
            if (dbOptions != null) dbOptions.close();
        } catch (Exception e) {
            log.warn("Failed to close DBOptions", e);
        }
        for (ColumnFamilyOptions options : columnFamilyOptions) {
            try {
                if (options != null) options.close();
            } catch (Exception e) {
                log.warn("Failed to close ColumnFamilyOptions", e);
            }
        }
        columnFamilyOptions.clear();

        if (destroyFiles && dbPath != null) {
            try (Options options = new Options()) {
                RocksDB.destroyDB(dbPath, options);
            } catch (RocksDBException e) {
                throw new RocksDBRuntimeException("Failed to destroy RocksDB at: " + dbPath, e);
            }
        }
    }
}
