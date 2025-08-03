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

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableList;

import org.apache.seatunnel.common.exception.CommonErrorCode;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCommonConfig;
import org.apache.seatunnel.connectors.seatunnel.iceberg.exception.IcebergConnectorException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.common.DynClasses;
import org.apache.iceberg.common.DynMethods;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class IcebergCatalogLoader implements Serializable {

    private static final long serialVersionUID = -6003040601422350869L;
    private static final List<String> HADOOP_CONF_FILES =
            ImmutableList.of("core-site.xml", "hdfs-site.xml", "hive-site.xml");
    private final IcebergCommonConfig config;

    public IcebergCatalogLoader(IcebergCommonConfig config) {
        this.config = config;
    }

    public Catalog loadCatalog() {
        // When using the SeaTunnel engine, set the current class loader to prevent loading failures
        Thread.currentThread().setContextClassLoader(IcebergCatalogLoader.class.getClassLoader());

        // Prepare catalog properties with REST-specific configurations
        Map<String, String> catalogProps = prepareCatalogProperties();

        return CatalogUtil.buildIcebergCatalog(
                config.getCatalogName(), catalogProps, loadHadoopConfig(config));
    }

    private Map<String, String> prepareCatalogProperties() {
        Map<String, String> catalogProps = new HashMap<>();
        log.info("Preparing catalog properties for REST catalog");

        // Add base catalog properties if provided
        if (config.getCatalogProps() != null) {
            catalogProps.putAll(config.getCatalogProps());
            log.info("Added base catalog properties: {}", config.getCatalogProps());
        }

        // Add REST catalog specific properties
        if (config.getRestUri() != null) {
            catalogProps.put("uri", config.getRestUri());
            log.info("REST URI configured: {}", config.getRestUri());
        } else {
            log.warn("REST URI is not configured");
        }

        if (config.getRestWarehouse() != null) {
            catalogProps.put("warehouse", config.getRestWarehouse());
            log.info("REST warehouse configured: {}", config.getRestWarehouse());
        } else {
            log.warn("REST warehouse is not configured");
        }

        // Handle authentication based on auth type
        String authType = config.getRestAuthType();
        log.info("REST authentication type: {}", authType);

        if ("aws".equals(authType)) {
            log.info("Setting up AWS authentication");
            setupAwsAuthentication(catalogProps);
        } else if ("token".equals(authType) && config.getRestAuthToken() != null) {
            catalogProps.put("token", config.getRestAuthToken());
            log.info("Token authentication configured");
        } else if ("none".equals(authType)) {
            log.info("No authentication configured");
        } else {
            log.warn("Unknown or unsupported authentication type: {}", authType);
        }

        log.info(
                "Final catalog properties (excluding secrets): {}",
                catalogProps.entrySet().stream()
                        .filter(
                                entry ->
                                        !entry.getKey().contains("secret")
                                                && !entry.getKey().contains("token"))
                        .collect(
                                java.util.stream.Collectors.toMap(
                                        java.util.Map.Entry::getKey,
                                        java.util.Map.Entry::getValue)));

        return catalogProps;
    }

    private void setupAwsAuthentication(Map<String, String> catalogProps) {
        log.info("Setting up AWS authentication for REST catalog");

        // Enable SigV4 signing for AWS REST catalog
        catalogProps.put("rest.sigv4-enabled", "true");
        log.info("Enabled SigV4 signing");

        // AWS credentials will be resolved through environment variables:
        // AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_SESSION_TOKEN (optional)
        // AWS_REGION or AWS_DEFAULT_REGION
        log.info("AWS credentials will be resolved through environment variables");

        // Determine signing service based on REST URI
        String restUri = config.getRestUri();
        if (restUri != null) {
            if (restUri.contains("s3tables")) {
                catalogProps.put("rest.signing-name", "s3tables");
                log.info("Signing service set to: s3tables for URI: {}", restUri);
            } else {
                catalogProps.put("rest.signing-name", "glue");
                log.info("Signing service set to: glue for URI: {}", restUri);
            }
        } else {
            catalogProps.put("rest.signing-name", "glue");
            log.warn("REST URI is null, defaulting signing service to: glue");
        }

        log.info("AWS authentication configured to use environment variables");
    }

    /** Loading Hadoop configuration through reflection */
    public Object loadHadoopConfig(IcebergCommonConfig config) {
        Class<?> configClass =
                DynClasses.builder()
                        .impl("org.apache.hadoop.hdfs.HdfsConfiguration")
                        .orNull()
                        .build();
        if (configClass == null) {
            configClass =
                    DynClasses.builder()
                            .impl("org.apache.hadoop.conf.Configuration")
                            .orNull()
                            .build();
        }

        if (configClass == null) {
            log.info("Hadoop not found on classpath, not creating Hadoop config");
            return null;
        }
        try {
            Object result = configClass.getDeclaredConstructor().newInstance();
            DynMethods.BoundMethod addResourceMethod =
                    DynMethods.builder("addResource").impl(configClass, URL.class).build(result);
            DynMethods.BoundMethod setMethod =
                    DynMethods.builder("set")
                            .impl(configClass, String.class, String.class)
                            .build(result);

            //  load any config files in the specified config directory
            String hadoopConfPath = config.getHadoopConfPath();
            if (hadoopConfPath != null) {
                HADOOP_CONF_FILES.forEach(
                        confFile -> {
                            Path path = Paths.get(hadoopConfPath, confFile);
                            if (Files.exists(path)) {
                                try {
                                    addResourceMethod.invoke(path.toUri().toURL());
                                } catch (IOException e) {
                                    log.warn(
                                            "Error adding Hadoop resource {}, resource was not added",
                                            path,
                                            e);
                                }
                            }
                        });
            }
            config.getHadoopProps().forEach(setMethod::invoke);
            // kerberos authentication
            doKerberosLogin((Configuration) result);
            log.info("Hadoop config initialized: {}", configClass.getName());
            return result;
        } catch (InstantiationException
                | IllegalAccessException
                | NoSuchMethodException
                | InvocationTargetException e) {
            log.warn(
                    "Hadoop found on classpath but could not create config, proceeding without config",
                    e);
        }
        return null;
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
                UserGroupInformation.loginUserFromKeytab(principal, keytabPath);
                UserGroupInformation loginUser = UserGroupInformation.getLoginUser();
                log.info("Kerberos authentication successful,UGI {}", loginUser);
            } catch (IOException e) {
                throw new SeaTunnelException("check connectivity failed, " + e.getMessage(), e);
            }
        }
    }
}
