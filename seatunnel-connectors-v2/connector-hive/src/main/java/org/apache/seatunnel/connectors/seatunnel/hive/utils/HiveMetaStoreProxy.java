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

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableList;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopLoginFactory;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.config.HdfsSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.catalog.HiveTable;
import org.apache.seatunnel.connectors.seatunnel.hive.catalog.HiveTypeConvertor;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

@Slf4j
public class HiveMetaStoreProxy {
    private HiveMetaStoreClient hiveMetaStoreClient;
    private static volatile HiveMetaStoreProxy INSTANCE = null;
    private static final List<String> HADOOP_CONF_FILES = ImmutableList.of("hive-site.xml");

    private HiveMetaStoreProxy(ReadonlyConfig readonlyConfig) {
        String metastoreUri = readonlyConfig.get(HiveConfig.METASTORE_URI);
        String hiveHadoopConfigPath = readonlyConfig.get(HiveConfig.HADOOP_CONF_PATH);
        String hiveSitePath = readonlyConfig.get(HiveConfig.HIVE_SITE_PATH);
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", metastoreUri);
        try {
            if (StringUtils.isNotBlank(hiveHadoopConfigPath)) {
                HADOOP_CONF_FILES.forEach(
                        confFile -> {
                            java.nio.file.Path path = Paths.get(hiveHadoopConfigPath, confFile);
                            if (Files.exists(path)) {
                                try {
                                    hiveConf.addResource(path.toUri().toURL());
                                } catch (IOException e) {
                                    log.warn(
                                            "Error adding Hadoop resource {}, resource was not added",
                                            path,
                                            e);
                                }
                            }
                        });
            }

            if (StringUtils.isNotBlank(hiveSitePath)) {
                hiveConf.addResource(new File(hiveSitePath).toURI().toURL());
            }

            log.info("hive client conf:{}", hiveConf);
            if (HiveMetaStoreProxyUtils.enableKerberos(readonlyConfig)) {
                // login Kerberos
                Configuration authConf = new Configuration();
                authConf.set("hadoop.security.authentication", "kerberos");
                this.hiveMetaStoreClient =
                        HadoopLoginFactory.loginWithKerberos(
                                authConf,
                                readonlyConfig.get(HdfsSourceConfigOptions.KRB5_PATH),
                                readonlyConfig.get(HdfsSourceConfigOptions.KERBEROS_PRINCIPAL),
                                readonlyConfig.get(HdfsSourceConfigOptions.KERBEROS_KEYTAB_PATH),
                                (conf, userGroupInformation) -> {
                                    return new HiveMetaStoreClient(hiveConf);
                                });
                return;
            }
            if (HiveMetaStoreProxyUtils.enableRemoteUser(readonlyConfig)) {
                this.hiveMetaStoreClient =
                        HadoopLoginFactory.loginWithRemoteUser(
                                new Configuration(),
                                readonlyConfig.get(HdfsSourceConfigOptions.REMOTE_USER),
                                (conf, userGroupInformation) -> {
                                    return new HiveMetaStoreClient(hiveConf);
                                });
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
                            metastoreUri, readonlyConfig.get(HiveConfig.HIVE_SITE_PATH));
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

    public HiveTable getTableInfo(@NonNull String dbName, @NonNull String tableName) {
        Table table = getTable(dbName, tableName);
        TableSchema.Builder builder = new TableSchema.Builder();
        List<FieldSchema> cols = table.getSd().getCols();
        cols.forEach(
                col -> {
                    builder.column(
                            PhysicalColumn.of(
                                    col.getName(),
                                    HiveTypeConvertor.covertHiveTypeToSeaTunnelType(
                                            col.getName(), col.getType()),
                                    (Long) null,
                                    true,
                                    null,
                                    col.getComment()));
                });
        List<String> partitionKeys = new ArrayList<>();
        table.getPartitionKeys()
                .forEach(
                        p -> {
                            builder.column(
                                    PhysicalColumn.of(
                                            p.getName(),
                                            HiveTypeConvertor.covertHiveTypeToSeaTunnelType(
                                                    p.getName(), p.getType()),
                                            (Long) null,
                                            true,
                                            null,
                                            p.getComment()));
                            partitionKeys.add(p.getName());
                        });
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("Hive", dbName, tableName),
                        builder.build(),
                        Collections.emptyMap(),
                        partitionKeys,
                        null);
        return HiveTable.of(
                catalogTable,
                table.getParameters(),
                table.getSd().getInputFormat(),
                table.getSd().getLocation());
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
