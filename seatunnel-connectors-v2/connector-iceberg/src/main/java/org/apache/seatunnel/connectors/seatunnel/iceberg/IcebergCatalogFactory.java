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

package org.apache.seatunnel.connectors.seatunnel.iceberg;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCatalogType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.hadoop.SerializableConfiguration;
import org.apache.iceberg.hive.HiveCatalog;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import static org.apache.hadoop.security.authorize.ProxyServers.refresh;

@Slf4j
public class IcebergCatalogFactory implements Serializable {

    private static final long serialVersionUID = -6003040601422350869L;

    private final String catalogName;
    private final IcebergCatalogType catalogType;
    private final String warehouse;
    private final String uri;

    // for kerberos
    private String kerberosPrincipal;
    private String kerberosKrb5ConfPath;
    private String kerberosKeytabPath;
    private String hdfsSitePath;
    private String hiveSitePath;

    public IcebergCatalogFactory(
            @NonNull String catalogName,
            @NonNull IcebergCatalogType catalogType,
            @NonNull String warehouse,
            String uri) {
        this.catalogName = catalogName;
        this.catalogType = catalogType;
        this.warehouse = warehouse;
        this.uri = uri;
    }

    public IcebergCatalogFactory(
            @NonNull String catalogName,
            @NonNull IcebergCatalogType catalogType,
            @NonNull String warehouse,
            String uri,
            String kerberosPrincipal,
            String kerberosKrb5ConfPath,
            String kerberosKeytabPath,
            String hdfsSitePath,
            String hiveSitePath) {
        this.catalogName = catalogName;
        this.catalogType = catalogType;
        this.warehouse = warehouse;
        this.kerberosPrincipal = kerberosPrincipal;
        this.kerberosKrb5ConfPath = kerberosKrb5ConfPath;
        this.kerberosKeytabPath = kerberosKeytabPath;
        this.hdfsSitePath = hdfsSitePath;
        this.hiveSitePath = hiveSitePath;
        this.uri = uri;
    }

    public Catalog create() {
        Configuration conf = new Configuration();
        conf.set("dfs.client.use.datanode.hostname", "true");
        Map<String, String> properties = new HashMap<>();
        properties.put(CatalogProperties.WAREHOUSE_LOCATION, warehouse);
        SerializableConfiguration serializableConf =
                new SerializableConfiguration(doKerberosLogin(conf));
        switch (catalogType) {
            case HADOOP:
                return hadoop(catalogName, serializableConf, properties);
            case HIVE:
                properties.put(CatalogProperties.URI, uri);
                return hive(catalogName, serializableConf, properties);
            default:
                throw new IcebergConnectorException(
                        CommonErrorCode.UNSUPPORTED_OPERATION,
                        String.format("Unsupported catalogType: %s", catalogType));
        }
    }

    private Configuration doKerberosLogin(Configuration configuration) {
        if (StringUtils.isNotEmpty(kerberosPrincipal)
                && StringUtils.isNotEmpty(kerberosKrb5ConfPath)
                && StringUtils.isNotEmpty(kerberosKeytabPath)) {
            try {
                // login Kerberos
                if (StringUtils.isNotEmpty(hdfsSitePath)) {
                    configuration.addResource(new File(hdfsSitePath).toURI().toURL());
                }
                if (StringUtils.isNotEmpty(hiveSitePath)) {
                    configuration.addResource(new File(hiveSitePath).toURI().toURL());
                }

                System.setProperty("java.security.krb5.conf", kerberosKrb5ConfPath);
                System.setProperty("krb.principal", kerberosPrincipal);
                doKerberosAuthentication(configuration, kerberosPrincipal, kerberosKeytabPath);

            } catch (Exception e) {
                throw new IcebergConnectorException(
                        CommonErrorCode.KERBEROS_AUTHORIZED_FAILED,
                        String.format("Kerberos authentication failed: %s", e.getMessage()));
            }
        } else {
            log.warn(
                    "Kerberos authentication is not configured, it will skip kerberos authentication");
        }

        return configuration;
    }

    public static void doKerberosAuthentication(
            Configuration configuration, String principal, String keytabPath) {
        if (StringUtils.isBlank(principal) || StringUtils.isBlank(keytabPath)) {
            log.warn(
                    "Principal [{}] or keytabPath [{}] is empty, it will skip kerberos authentication",
                    principal,
                    keytabPath);
        } else {
            configuration.set("hadoop.security.authentication", "kerberos");
            UserGroupInformation.setConfiguration(configuration);
            try {
                log.info(
                        "Start Kerberos authentication using principal {} and keytab {}",
                        principal,
                        keytabPath);
                refresh();
                UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
                log.info("Kerberos authentication successful");
            } catch (IOException e) {
                throw new SeaTunnelException("check connectivity failed, " + e.getMessage(), e);
            }
        }
    }

    private static Catalog hadoop(
            String catalogName, SerializableConfiguration conf, Map<String, String> properties) {
        return CatalogUtil.loadCatalog(
                HadoopCatalog.class.getName(), catalogName, properties, conf.get());
    }

    private static Catalog hive(
            String catalogName, SerializableConfiguration conf, Map<String, String> properties) {
        return CatalogUtil.loadCatalog(
                HiveCatalog.class.getName(), catalogName, properties, conf.get());
    }
}
