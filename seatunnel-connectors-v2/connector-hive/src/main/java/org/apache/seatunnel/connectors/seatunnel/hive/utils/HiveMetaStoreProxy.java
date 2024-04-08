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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.BaseSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopLoginFactory;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hive.source.config.HiveSourceOptions;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
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
    private HiveMetaStoreClient hiveMetaStoreClient;
    private static volatile HiveMetaStoreProxy INSTANCE = null;

    private HiveMetaStoreProxy(ReadonlyConfig readonlyConfig) {
        String metastoreUri = readonlyConfig.get(HiveSourceOptions.METASTORE_URI);
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", metastoreUri);
        try {
            if (StringUtils.isNotEmpty(readonlyConfig.get(HiveSourceOptions.HIVE_SITE_PATH))) {
                String hiveSitePath = readonlyConfig.get(HiveSourceOptions.HIVE_SITE_PATH);
                hiveConf.addResource(new File(hiveSitePath).toURI().toURL());
            }
            if (HiveMetaStoreProxyUtils.enableKerberos(readonlyConfig)) {
                this.hiveMetaStoreClient =
                        HadoopLoginFactory.loginWithKerberos(
                                new Configuration(),
                                readonlyConfig.get(BaseSourceConfigOptions.KRB5_PATH),
                                readonlyConfig.get(BaseSourceConfigOptions.KERBEROS_PRINCIPAL),
                                readonlyConfig.get(BaseSourceConfigOptions.KERBEROS_KEYTAB_PATH),
                                (configuration, userGroupInformation) ->
                                        new HiveMetaStoreClient(hiveConf));
                return;
            }
            if (HiveMetaStoreProxyUtils.enableRemoteUser(readonlyConfig)) {
                this.hiveMetaStoreClient =
                        HadoopLoginFactory.loginWithRemoteUser(
                                new Configuration(),
                                readonlyConfig.get(BaseSourceConfigOptions.REMOTE_USER),
                                (configuration, userGroupInformation) ->
                                        new HiveMetaStoreClient(hiveConf));
                return;
            }
            this.hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
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
                            metastoreUri, readonlyConfig.get(HiveSourceOptions.HIVE_SITE_PATH));
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.INITIALIZE_HIVE_METASTORE_CLIENT_FAILED, errorMsg, e);
        } catch (Exception e) {
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.INITIALIZE_HIVE_METASTORE_CLIENT_FAILED,
                    "Login form kerberos failed",
                    e);
        }
    }

    public static HiveMetaStoreProxy getInstance(ReadonlyConfig readonlyConfig) {
        if (INSTANCE == null) {
            synchronized (HiveMetaStoreProxy.class) {
                if (INSTANCE == null) {
                    INSTANCE = new HiveMetaStoreProxy(readonlyConfig);
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
            try {
                hiveMetaStoreClient.appendPartition(dbName, tableName, partition);
            } catch (AlreadyExistsException e) {
                log.warn("The partition {} are already exists", partition);
            }
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
