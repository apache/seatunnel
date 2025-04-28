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
import org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopLoginFactory;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.config.HdfsSourceConfigOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveConfig;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveOptions;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;

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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

@Slf4j
public class HiveMetaStoreProxy implements Closeable, Serializable {
    private static final long serialVersionUID = 1L;

    private static final List<String> HADOOP_CONF_FILES = ImmutableList.of("hive-site.xml");

    private transient HiveMetaStoreClient hiveMetaStoreClient;

    private final String metastoreUri;
    private final String hiveHadoopConfigPath;
    private final String hiveSitePath;
    private final String krb5Path;
    private final String kerberosPrincipal;
    private final String kerberosKeytabPath;
    private final String remoteUser;

    public HiveMetaStoreProxy(ReadonlyConfig readonlyConfig) {
        metastoreUri = readonlyConfig.get(HiveOptions.METASTORE_URI);
        hiveHadoopConfigPath = readonlyConfig.get(HiveConfig.HADOOP_CONF_PATH);
        hiveSitePath = readonlyConfig.get(HiveConfig.HIVE_SITE_PATH);
        krb5Path = readonlyConfig.get(HdfsSourceConfigOptions.KRB5_PATH);
        kerberosPrincipal = readonlyConfig.get(HdfsSourceConfigOptions.KERBEROS_PRINCIPAL);
        kerberosKeytabPath = readonlyConfig.get(HdfsSourceConfigOptions.KERBEROS_KEYTAB_PATH);
        remoteUser = readonlyConfig.get(HdfsSourceConfigOptions.REMOTE_USER);
    }

    private synchronized HiveMetaStoreClient getClient() {
        if (hiveMetaStoreClient != null) {
            return hiveMetaStoreClient;
        }

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
            if (enableKerberos()) {
                // login Kerberos
                Configuration authConf = new Configuration();
                authConf.set("hadoop.security.authentication", "kerberos");
                this.hiveMetaStoreClient =
                        HadoopLoginFactory.loginWithKerberos(
                                authConf,
                                krb5Path,
                                kerberosPrincipal,
                                kerberosKeytabPath,
                                (conf, userGroupInformation) -> {
                                    return new HiveMetaStoreClient(hiveConf);
                                });
                return hiveMetaStoreClient;
            }
            if (enableRemoteUser()) {
                this.hiveMetaStoreClient =
                        HadoopLoginFactory.loginWithRemoteUser(
                                new Configuration(),
                                remoteUser,
                                (conf, userGroupInformation) -> {
                                    return new HiveMetaStoreClient(hiveConf);
                                });
                return hiveMetaStoreClient;
            }
            this.hiveMetaStoreClient = new HiveMetaStoreClient(hiveConf);
            return hiveMetaStoreClient;
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
                            metastoreUri, hiveSitePath);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.INITIALIZE_HIVE_METASTORE_CLIENT_FAILED, errorMsg, e);
        } catch (Exception e) {
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.INITIALIZE_HIVE_METASTORE_CLIENT_FAILED,
                    "Login form kerberos failed",
                    e);
        }
    }

    public Table getTable(@NonNull String dbName, @NonNull String tableName) {
        try {
            return getClient().getTable(dbName, tableName);
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
                getClient().appendPartition(dbName, tableName, partition);
            } catch (AlreadyExistsException e) {
                log.warn("The partition {} are already exists", partition);
            }
        }
    }

    public void dropPartitions(
            @NonNull String dbName, @NonNull String tableName, List<String> partitions)
            throws TException {
        for (String partition : partitions) {
            getClient().dropPartition(dbName, tableName, partition, false);
        }
    }

    @Override
    public synchronized void close() {
        if (Objects.nonNull(hiveMetaStoreClient)) {
            hiveMetaStoreClient.close();
            hiveMetaStoreClient = null;
        }
    }

    private boolean enableKerberos() {
        return StringUtils.isNotBlank(kerberosPrincipal)
                && StringUtils.isNotBlank(kerberosKeytabPath);
    }

    private boolean enableRemoteUser() {
        return StringUtils.isNotBlank(remoteUser);
    }
}
