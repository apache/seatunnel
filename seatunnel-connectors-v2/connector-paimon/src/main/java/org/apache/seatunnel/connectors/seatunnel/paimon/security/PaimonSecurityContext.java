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

package org.apache.seatunnel.connectors.seatunnel.paimon.security;

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableList;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonHadoopConfiguration;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.options.Options;
import org.apache.paimon.security.SecurityConfiguration;
import org.apache.paimon.security.SecurityContext;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

@Slf4j
public class PaimonSecurityContext extends SecurityContext {
    private static final String KRB5_CONF_KEY = "java.security.krb5.conf";
    private static final String FS_DISABLE_CACHE = "fs.hdfs.impl.disable.cache";
    private static final List<String> HADOOP_CONF_FILES =
            ImmutableList.of("core-site.xml", "hdfs-site.xml", "hive-site.xml");

    public static void shouldEnableKerberos(Configuration configuration) {
        String kerberosPrincipal =
                configuration.get(SecurityConfiguration.KERBEROS_LOGIN_PRINCIPAL.key());
        String kerberosKeytabFilePath =
                configuration.get(SecurityConfiguration.KERBEROS_LOGIN_KEYTAB.key());
        if (StringUtils.isNotBlank(kerberosPrincipal)
                && StringUtils.isNotBlank(kerberosKeytabFilePath)) {
            configuration.set("hadoop.security.authentication", "kerberos");
            PaimonSecurityContext.verifyKerberosAuthentication(configuration);
        }
    }

    /**
     * Loading Hadoop configuration by hadoop conf path or props set by paimon.hadoop.conf
     *
     * @return
     */
    public static PaimonHadoopConfiguration loadHadoopConfig(PaimonConfig paimonConfig) {
        PaimonHadoopConfiguration configuration = new PaimonHadoopConfiguration();
        String hdfsSitePath = paimonConfig.getHdfsSitePath();
        if (StringUtils.isNotBlank(hdfsSitePath)) {
            configuration.addResource(new Path(hdfsSitePath));
        }
        String hadoopConfPath = paimonConfig.getHadoopConfPath();
        if (StringUtils.isNotBlank(hadoopConfPath)) {
            HADOOP_CONF_FILES.forEach(
                    confFile -> {
                        java.nio.file.Path path = Paths.get(hadoopConfPath, confFile);
                        if (Files.exists(path)) {
                            try {
                                configuration.addResource(path.toUri().toURL());
                            } catch (IOException e) {
                                log.warn(
                                        "Error adding Hadoop resource {}, resource was not added",
                                        path,
                                        e);
                            }
                        }
                    });
        }
        paimonConfig.getHadoopConfProps().forEach((k, v) -> configuration.set(k, v));
        // This configuration is enabled to avoid affecting other hadoop filesystem jobs
        // refer:
        // org.apache.seatunnel.connectors.seatunnel.file.hadoop.HadoopFileSystemProxy.createConfiguration
        configuration.setBoolean(FS_DISABLE_CACHE, true);
        log.info("Hadoop config initialized: {}", configuration.getClass().getName());
        return configuration;
    }

    /**
     * Check if we need to verify kerberos authentication
     *
     * @param configuration
     */
    public static void verifyKerberosAuthentication(Configuration configuration) {
        String principalKey = SecurityConfiguration.KERBEROS_LOGIN_PRINCIPAL.key();
        String keytabKey = SecurityConfiguration.KERBEROS_LOGIN_KEYTAB.key();
        String kerberosPrincipal = configuration.get(principalKey);
        String kerberosKeytabFilePath = configuration.get(keytabKey);
        String krb5Conf = configuration.get(KRB5_CONF_KEY);
        Options options = new Options();
        options.set(principalKey, kerberosPrincipal);
        options.set(keytabKey, kerberosKeytabFilePath);
        String ticketCacheKey = SecurityConfiguration.KERBEROS_LOGIN_USETICKETCACHE.key();
        boolean ticketCache =
                configuration.getBoolean(
                        ticketCacheKey,
                        SecurityConfiguration.KERBEROS_LOGIN_USETICKETCACHE.defaultValue());
        options.set(ticketCacheKey, String.valueOf(ticketCache));
        try {
            CatalogContext catalogContext = CatalogContext.create(options, configuration);
            if (StringUtils.isNotBlank(krb5Conf)) {
                reloadKrb5conf(krb5Conf);
            }
            // refer: https://paimon.apache.org/docs/master/filesystems/hdfs/#kerberos.
            // If the keytab is blank or principal is blank or keytabFile is not exists, the method
            // of install will not perform kerberos authentication without any exception.
            install(catalogContext);
        } catch (Exception e) {
            throw new PaimonConnectorException(
                    PaimonConnectorErrorCode.AUTHENTICATE_KERBEROS_FAILED,
                    "Failed to login user from keytab : "
                            + kerberosKeytabFilePath
                            + " and kerberos principal : "
                            + kerberosPrincipal,
                    e);
        }
    }

    private static void reloadKrb5conf(String krb5conf) {
        System.setProperty(KRB5_CONF_KEY, krb5conf);
        try {
            refreshJdkKerberosConfig();
            KerberosName.resetDefaultRealm();
        } catch (IllegalAccessException e) {
            // Module access denied: the JVM is missing the jgss export, so the custom krb5.conf
            // configured for this catalog cannot take effect. This is an actionable deployment
            // problem, not a transient refresh failure, so report it at ERROR with the fix.
            log.error(
                    "JVM module system denied the Kerberos configuration reload, so the configured"
                            + " krb5.conf will NOT take effect. Start the JVM with"
                            + " --add-exports=java.security.jgss/sun.security.krb5=ALL-UNNAMED"
                            + " (shipped in config/jvm_*_options and appended by the launch"
                            + " scripts since the Java 11 baseline).",
                    e);
        } catch (ReflectiveOperationException e) {
            log.warn(
                    "resetting default realm failed, current default realm will still be used.", e);
        }
    }

    /**
     * Refreshes the JDK Kerberos configuration so that the krb5.conf just set takes effect.
     *
     * <p>Invoked reflectively because sun.security.krb5.Config lives in the java.security.jgss
     * module, which does not export it to the unnamed module, so importing it directly fails to
     * compile on Java 17. A failure here leaves the default realm untouched, which matches the
     * previous behaviour when Config.refresh() threw.
     */
    private static void refreshJdkKerberosConfig() throws ReflectiveOperationException {
        Class.forName("sun.security.krb5.Config").getMethod("refresh").invoke(null);
    }
}
