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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.file.sink.util.FileSystemUtils;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.net.MalformedURLException;
import java.util.List;
import java.util.Objects;

@Slf4j
public class HiveMetaStoreProxy {
    private final HiveMetaStoreClient hiveMetaStoreClient;
    private static volatile HiveMetaStoreProxy INSTANCE = null;

    private HiveMetaStoreProxy(Config config) {
        String metastoreUri = config.getString(HiveConfig.METASTORE_URI.key());
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", metastoreUri);
        if (config.hasPath(BaseSourceConfig.KERBEROS_PRINCIPAL.key())
                && config.hasPath(BaseSourceConfig.KERBEROS_KEYTAB_PATH.key())) {
            String principal = config.getString(BaseSourceConfig.KERBEROS_PRINCIPAL.key());
            String keytabPath = config.getString(BaseSourceConfig.KERBEROS_KEYTAB_PATH.key());
            Configuration configuration = new Configuration();
            FileSystemUtils.doKerberosAuthentication(configuration, principal, keytabPath);
        }
        try {
            if (config.hasPath(HiveConfig.HIVE_SITE_PATH.key())) {
                String hiveSitePath = config.getString(HiveConfig.HIVE_SITE_PATH.key());
                hiveConf.addResource(new File(hiveSitePath).toURI().toURL());
            }
            hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
        } catch (MetaException e) {
            String errorMsg =
                    String.format(
                            "Using this hive uris [%s] to initialize "
                                    + "hive metastore client instance failed",
                            metastoreUri);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.INITIALIZE_HIVE_METASTORE_CLIENT_FAILED, errorMsg, e);
        } catch (MalformedURLException e) {
            String errorMsg =
                    String.format(
                            "Using this hive uris [%s], hive conf [%s] to initialize "
                                    + "hive metastore client instance failed",
                            metastoreUri, config.getString(HiveConfig.HIVE_SITE_PATH.key()));
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.INITIALIZE_HIVE_METASTORE_CLIENT_FAILED, errorMsg, e);
        }
    }

    public static HiveMetaStoreProxy getInstance(Config config) {
        if (INSTANCE == null) {
            synchronized (HiveMetaStoreProxy.class) {
                if (INSTANCE == null) {
                    INSTANCE = new HiveMetaStoreProxy(config);
                }
            }
        }
        return INSTANCE;
    }

    public Table getTable(@NonNull String dbName, @NonNull String tableName) {
        try {
            return hiveMetaStoreClient.getTable(dbName, tableName);
        } catch (TException e) {
            String errorMsg =
                    String.format("Get table [%s.%s] information failed", dbName, tableName);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HIVE_TABLE_INFORMATION_FAILED, errorMsg, e);
        }
    }

    public void addPartitions(
            @NonNull String dbName, @NonNull String tableName, List<String> partitions)
            throws TException {
        for (String partition : partitions) {
            hiveMetaStoreClient.appendPartition(dbName, tableName, partition);
        }
    }

    public void dropPartitions(
            @NonNull String dbName, @NonNull String tableName, List<String> partitions)
            throws TException {
        for (String partition : partitions) {
            hiveMetaStoreClient.dropPartition(dbName, tableName, partition, false);
        }
    }

    public synchronized void close() {
        if (Objects.nonNull(hiveMetaStoreClient)) {
            hiveMetaStoreClient.close();
            HiveMetaStoreProxy.INSTANCE = null;
        }
    }
}
