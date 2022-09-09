/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hive.utils;

import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.NonNull;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.util.List;
import java.util.Objects;

public class HiveMetaStoreProxy {
    private final HiveMetaStoreClient hiveMetaStoreClient;
    private static HiveMetaStoreProxy INSTANCE = null;

    private HiveMetaStoreProxy(@NonNull String uris) {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", uris);
        try {
            hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            throw new RuntimeException(e);
        }
    }

    public static synchronized HiveMetaStoreProxy getInstance(Config config) {
        if (INSTANCE == null) {
            String metastoreUri = config.getString(HiveConfig.METASTORE_URI);
            INSTANCE = new HiveMetaStoreProxy(metastoreUri);
        }
        return INSTANCE;
    }

    public Table getTable(@NonNull String dbName, @NonNull String tableName) {
        try {
            return hiveMetaStoreClient.getTable(dbName, tableName);
        } catch (TException e) {
            String errorMsg = String.format("Get table [%s.%s] information failed", dbName, tableName);
            throw new RuntimeException(errorMsg, e);
        }
    }

    public List<FieldSchema> getTableFields(@NonNull String dbName, @NonNull String tableName) {
        try {
            return hiveMetaStoreClient.getFields(dbName, tableName);
        } catch (TException e) {
            String errorMsg = String.format("Get table [%s.%s] fields information failed", dbName, tableName);
            throw new RuntimeException(errorMsg, e);
        }
    }

    public synchronized void close() {
        if (Objects.nonNull(hiveMetaStoreClient)) {
            hiveMetaStoreClient.close();
            HiveMetaStoreProxy.INSTANCE = null;
        }
    }
}
