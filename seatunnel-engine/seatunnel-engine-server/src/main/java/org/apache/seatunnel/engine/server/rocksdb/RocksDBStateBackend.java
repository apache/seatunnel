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

import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.server.MapStoreConfig;
import org.apache.seatunnel.engine.imap.storage.api.RocksDBStorageFactory;
import org.apache.seatunnel.engine.serializer.protobuf.ProtoStuffSerializer;
import org.apache.seatunnel.engine.server.persistence.FileMapStoreManager;

import org.apache.commons.lang.StringUtils;

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

    private final RocksDB db;
    private final DBOptions dbOptions;
    private final List<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>();
    private final List<ColumnFamilyHandle> cfHandles = new ArrayList<>();
    private final Map<String, ColumnFamilyHandle> columnFamilies = new HashMap<>();
    private final Map<String, RocksDBValueState<Object, Object>> valueStateMap = new HashMap<>();
    private final List<String> initialStateNames =
            Arrays.asList(
                    Constant.IMAP_RUNNING_JOB_METRICS
                    // Add other initial state names here
                    );

    private FileMapStoreManager fileMapStoreManager;

    public RocksDBStateBackend(
            String dbPath, RocksDBStorageFactory factory, MapStoreConfig mapStoreConfig)
            throws RocksDBException {
        RocksDB.loadLibrary();
        try {
            this.dbOptions =
                    new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
            List<ColumnFamilyDescriptor> descriptors = new ArrayList<>();
            ColumnFamilyOptions defaultOptions = new ColumnFamilyOptions();
            descriptors.add(
                    new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, defaultOptions));
            this.columnFamilyOptions.add(defaultOptions);

            for (String name : initialStateNames) {
                if (StringUtils.isBlank(name)) continue;
                ColumnFamilyOptions options = new ColumnFamilyOptions();
                this.columnFamilyOptions.add(options);
                descriptors.add(
                        new ColumnFamilyDescriptor(name.getBytes(StandardCharsets.UTF_8), options));
            }

            this.db = RocksDB.open(dbOptions, dbPath, descriptors, cfHandles);
            initializeColumnFamiliesAndValueStates();

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

    private void initializeColumnFamiliesAndValueStates() {
        if (!cfHandles.isEmpty()) {
            ColumnFamilyHandle defaultHandle = cfHandles.get(0);
            columnFamilies.put("default", defaultHandle);
            valueStateMap.put(
                    "default",
                    new RocksDBValueState<>(db, defaultHandle, new ProtoStuffSerializer()));
        }
        int idx = 1;
        for (String name : initialStateNames) {
            if (StringUtils.isBlank(name)) continue;
            if (idx < cfHandles.size()) {
                columnFamilies.put(name, cfHandles.get(idx));
            }
            idx++;
        }

        for (String name : initialStateNames) {
            if (StringUtils.isBlank(name)) continue;
            RocksDBValueState<Object, Object> valueState =
                    new RocksDBValueState<>(
                            db, columnFamilies.get(name), new ProtoStuffSerializer());
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
        valueState.put(key, value);
        if (fileMapStoreManager != null) fileMapStoreManager.put(stateName, key, value);
    }

    public <K, V> void remove(String stateName, K key) {
        RocksDBValueState<K, V> valueState = getValueState(stateName);
        valueState.remove(key);
        if (fileMapStoreManager != null) fileMapStoreManager.remove(stateName, key);
    }

    public void close(String dbPath) {
        close(dbPath, true);
    }

    public void close(String dbPath, boolean destroyFiles) {
        if (fileMapStoreManager != null) this.fileMapStoreManager.destroy();

        for (State state : valueStateMap.values()) {
            state.close();
        }
        valueStateMap.clear();
        cfHandles.clear();
        columnFamilies.clear();

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
