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
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Objects;

@Slf4j
public class HiveMetaStoreProxy implements Closeable, Serializable {
    private static final List<String> HADOOP_CONF_FILES = ImmutableList.of("hive-site.xml");

    private final String metastoreUri;
    private final String hadoopConfDir;
    private final String hiveSitePath;
    private final boolean kerberosEnabled;
    private final boolean remoteUserEnabled;

    private final String krb5Path;
    private final String principal;
    private final String keytabPath;
    private final String remoteUser;

    private transient HiveMetaStoreClient hiveClient;

    public HiveMetaStoreProxy(ReadonlyConfig config) {
        this.metastoreUri = config.get(HiveOptions.METASTORE_URI);
        this.hadoopConfDir = config.get(HiveConfig.HADOOP_CONF_PATH);
        this.hiveSitePath = config.get(HiveConfig.HIVE_SITE_PATH);
        this.kerberosEnabled = HiveMetaStoreProxyUtils.enableKerberos(config);
        this.remoteUserEnabled = HiveMetaStoreProxyUtils.enableRemoteUser(config);
        this.krb5Path = config.get(HdfsSourceConfigOptions.KRB5_PATH);
        this.principal = config.get(HdfsSourceConfigOptions.KERBEROS_PRINCIPAL);
        this.keytabPath = config.get(HdfsSourceConfigOptions.KERBEROS_KEYTAB_PATH);
        this.remoteUser = config.get(HdfsSourceConfigOptions.REMOTE_USER);
    }

    private synchronized HiveMetaStoreClient getClient() {
        if (hiveClient == null) {
            hiveClient = initializeClient();
        }
        return hiveClient;
    }

    private HiveMetaStoreClient initializeClient() {
        HiveConf hiveConf = buildHiveConf();
        try {
            if (kerberosEnabled) {
                return loginWithKerberos(hiveConf);
            }
            if (remoteUserEnabled) {
                return loginWithRemoteUser(hiveConf);
            }
            return new HiveMetaStoreClient(hiveConf);
        } catch (Exception e) {
            String errMsg =
                    String.format(
                            "Failed to initialize HiveMetaStoreClient [uris=%s, hiveSite=%s]",
                            metastoreUri, hiveSitePath);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.INITIALIZE_HIVE_METASTORE_CLIENT_FAILED, errMsg, e);
        }
    }

    private HiveConf buildHiveConf() {
        HiveConf hiveConf = new HiveConf();
        hiveConf.set("hive.metastore.uris", metastoreUri);

        if (StringUtils.isNotBlank(hadoopConfDir)) {
            for (String fileName : HADOOP_CONF_FILES) {
                Path path = Paths.get(hadoopConfDir, fileName);
                if (Files.exists(path)) {
                    try {
                        hiveConf.addResource(path.toUri().toURL());
                    } catch (IOException e) {
                        log.warn("Error adding Hadoop config {}", path, e);
                    }
                }
            }
        }
        if (StringUtils.isNotBlank(hiveSitePath)) {
            try {
                hiveConf.addResource(new File(hiveSitePath).toURI().toURL());
            } catch (MalformedURLException e) {
                log.warn("Invalid hiveSitePath {}", hiveSitePath, e);
            }
        }
        log.info("Hive client configuration: {}", hiveConf);
        return hiveConf;
    }

    private HiveMetaStoreClient loginWithKerberos(HiveConf hiveConf) throws Exception {
        Configuration authConf = new Configuration();
        authConf.set("hadoop.security.authentication", "kerberos");
        return HadoopLoginFactory.loginWithKerberos(
                authConf,
                krb5Path,
                principal,
                keytabPath,
                (conf, ugi) -> new HiveMetaStoreClient(hiveConf));
    }

    private HiveMetaStoreClient loginWithRemoteUser(HiveConf hiveConf) throws Exception {
        return HadoopLoginFactory.loginWithRemoteUser(
                new Configuration(), remoteUser, (conf, ugi) -> new HiveMetaStoreClient(hiveConf));
    }

    public Table getTable(@NonNull String dbName, @NonNull String tableName) {
        try {
            return getClient().getTable(dbName, tableName);
        } catch (TException e) {
            String msg = String.format("Failed to get table %s.%s", dbName, tableName);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HIVE_TABLE_INFORMATION_FAILED, msg, e);
        }
    }

    public void addPartitions(
            @NonNull String dbName, @NonNull String tableName, List<String> partitions)
            throws TException {
        for (String partition : partitions) {
            try {
                getClient().appendPartition(dbName, tableName, partition);
            } catch (AlreadyExistsException ae) {
                log.warn("Partition {} already exists", partition);
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
        if (Objects.nonNull(hiveClient)) {
            hiveClient.close();
        }
    }
}
