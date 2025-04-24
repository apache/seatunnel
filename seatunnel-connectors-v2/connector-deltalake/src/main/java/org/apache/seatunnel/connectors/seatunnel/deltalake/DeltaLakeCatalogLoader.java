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

package org.apache.seatunnel.connectors.seatunnel.deltalake;

import io.delta.kernel.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.deltalake.catalog.DeltaLakeCatalog;
import org.apache.seatunnel.connectors.seatunnel.deltalake.config.DeltaLakeCommonConfig;
import org.apache.seatunnel.connectors.seatunnel.deltalake.exception.DeltalakeConnectorException;
import org.apache.seatunnel.shade.com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;

@Slf4j
public class DeltaLakeCatalogLoader implements Serializable {

    private static final long serialVersionUID = -6003040601422350869L;
    private static final List<String> HADOOP_CONF_FILES =
            ImmutableList.of("core-site.xml", "hdfs-site.xml", "hive-site.xml");
    private final DeltaLakeCommonConfig config;

    public DeltaLakeCatalogLoader(DeltaLakeCommonConfig config) {
        this.config = config;
    }

    public Catalog loadCatalog() {
        // When using the SeaTunnel engine, set the current class loader to prevent loading failures
        Thread.currentThread().setContextClassLoader(DeltaLakeCatalogLoader.class.getClassLoader());
        return new DeltaLakeCatalog(config.getCatalogName(), config);
    }

    /**
     * kerberos authentication
     *
     * @param configuration Configuration
     */
    private Configuration doKerberosLogin(Configuration configuration) {
        String kerberosKrb5ConfPath = config.getKerberosKrb5ConfPath();
        String kerberosKeytabPath = config.getKerberosKeytabPath();
        String kerberosPrincipal = config.getKerberosPrincipal();

        if (StringUtils.isNotEmpty(kerberosPrincipal)
                && StringUtils.isNotEmpty(kerberosKrb5ConfPath)
                && StringUtils.isNotEmpty(kerberosKeytabPath)) {
            try {
                System.setProperty("java.security.krb5.conf", kerberosKrb5ConfPath);
                System.setProperty("krb.principal", kerberosPrincipal);
                doKerberosAuthentication(configuration, kerberosPrincipal, kerberosKeytabPath);
            } catch (Exception e) {
                throw new DeltalakeConnectorException(
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
                UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
                UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
                log.info("Kerberos authentication successful,UGI {}", loginUser);
            } catch (IOException e) {
                throw new SeaTunnelException("check connectivity failed, " + e.getMessage(), e);
            }
        }
    }

    public Table loadTable(TablePath tablePath) {
        return null;
    }
}
