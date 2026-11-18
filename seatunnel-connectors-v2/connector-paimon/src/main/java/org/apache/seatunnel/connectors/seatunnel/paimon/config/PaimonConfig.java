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

package org.apache.seatunnel.connectors.seatunnel.paimon.config;

import org.apache.seatunnel.shade.com.google.common.collect.ImmutableList;
import org.apache.seatunnel.shade.com.google.common.net.InetAddresses;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.paimon.catalog.PaimonCatalogEnum;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;

/**
 * Utility class to store configuration options, used by {@link SeaTunnelSource} and {@link
 * SeaTunnelSink}.
 */
@Getter
@Slf4j
public class PaimonConfig implements Serializable {

    protected String catalogName;
    protected PaimonCatalogEnum catalogType;
    protected String catalogUri;
    protected String warehouse;
    protected String namespace;
    protected String table;
    protected String hdfsSitePath;
    protected Map<String, String> hadoopConfProps;
    protected String hadoopConfPath;
    protected String user;
    protected String password;
    private final Map<String, String> restOptions = new HashMap<>();

    public PaimonConfig(ReadonlyConfig readonlyConfig) {
        this.catalogName =
                checkArgumentNotBlank(
                        readonlyConfig.get(PaimonBaseOptions.CATALOG_NAME),
                        PaimonBaseOptions.CATALOG_NAME.key());
        this.warehouse =
                checkArgumentNotBlank(
                        readonlyConfig.get(PaimonBaseOptions.WAREHOUSE),
                        PaimonBaseOptions.WAREHOUSE.key());
        this.namespace = readonlyConfig.get(PaimonBaseOptions.DATABASE);
        this.table = readonlyConfig.get(PaimonBaseOptions.TABLE);
        this.hdfsSitePath = readonlyConfig.get(PaimonBaseOptions.HDFS_SITE_PATH);
        this.hadoopConfProps = readonlyConfig.get(PaimonBaseOptions.HADOOP_CONF);
        this.hadoopConfPath = readonlyConfig.get(PaimonBaseOptions.HADOOP_CONF_PATH);
        this.catalogType = readonlyConfig.get(PaimonBaseOptions.CATALOG_TYPE);
        if (catalogType == PaimonCatalogEnum.HIVE || catalogType == PaimonCatalogEnum.REST) {
            this.catalogUri =
                    checkArgumentNotBlank(
                            readonlyConfig.get(PaimonBaseOptions.CATALOG_URI),
                            PaimonBaseOptions.CATALOG_URI.key());
        }
        if (catalogType == PaimonCatalogEnum.REST) {
            validateRestUri();
            String provider = readonlyConfig.get(PaimonBaseOptions.TOKEN_PROVIDER);
            if (!"bear".equals(provider) && !"dlf".equals(provider)) {
                throw new SeaTunnelException("Invalid token.provider: expected bear or dlf");
            }
            if ("bear".equals(provider)) {
                checkArgumentNotBlank(
                        readonlyConfig.get(PaimonBaseOptions.TOKEN), PaimonBaseOptions.TOKEN.key());
            } else {
                checkArgumentNotBlank(
                        readonlyConfig.get(PaimonBaseOptions.DLF_ACCESS_KEY_ID),
                        PaimonBaseOptions.DLF_ACCESS_KEY_ID.key());
                checkArgumentNotBlank(
                        readonlyConfig.get(PaimonBaseOptions.DLF_ACCESS_KEY_SECRET),
                        PaimonBaseOptions.DLF_ACCESS_KEY_SECRET.key());
            }
            for (Option<String> option :
                    Arrays.asList(
                            PaimonBaseOptions.TOKEN_PROVIDER, PaimonBaseOptions.TOKEN,
                            PaimonBaseOptions.DLF_ACCESS_KEY_ID,
                                    PaimonBaseOptions.DLF_ACCESS_KEY_SECRET,
                            PaimonBaseOptions.DLF_SECURITY_TOKEN, PaimonBaseOptions.DLF_REGION)) {
                String value = readonlyConfig.get(option);
                if (value != null) {
                    if (option == PaimonBaseOptions.DLF_SECURITY_TOKEN) {
                        validateSecurityToken(value);
                    }
                    restOptions.put(option.key(), value);
                }
            }
        }
        this.user = readonlyConfig.get(PaimonBaseOptions.USER);
        this.password = readonlyConfig.get(PaimonBaseOptions.PASSWORD);
    }

    private void validateRestUri() {
        URI uri;
        try {
            uri = new URI(catalogUri);
        } catch (URISyntaxException e) {
            // URI syntax errors include the input, which may contain credentials.
            throw invalidRestUri();
        }
        if (!("http".equalsIgnoreCase(uri.getScheme()) || "https".equalsIgnoreCase(uri.getScheme()))
                || StringUtils.isBlank(uri.getHost())
                || uri.getRawUserInfo() != null) {
            throw invalidRestUri();
        }
        if ("http".equalsIgnoreCase(uri.getScheme()) && !isLoopback(uri.getHost())) {
            log.warn(
                    "catalog_uri uses HTTP outside loopback; REST credentials will be sent in clear text. Use HTTPS.");
        }
    }

    private SeaTunnelException invalidRestUri() {
        return new SeaTunnelException(
                "Invalid catalog_uri: REST requires HTTP or HTTPS, a host, and no user information");
    }

    private boolean isLoopback(String host) {
        if ("localhost".equalsIgnoreCase(host)) {
            return true;
        }
        String address =
                host.startsWith("[") && host.endsWith("]")
                        ? host.substring(1, host.length() - 1)
                        : host;
        // Do not resolve DNS names during configuration validation.
        return InetAddresses.isInetAddress(address)
                && InetAddresses.forString(address).isLoopbackAddress();
    }

    private void validateSecurityToken(String value) {
        for (int i = 0; i < value.length(); i++) {
            char character = value.charAt(i);
            if (character != '\t' && (character < ' ' || character >= 127)) {
                // OkHttp includes invalid non-Authorization header values in its exception text.
                throw new SeaTunnelException(
                        "Invalid HTTP header characters in "
                                + PaimonBaseOptions.DLF_SECURITY_TOKEN.key());
            }
        }
    }

    protected String checkArgumentNotBlank(String propValue, String propKey) {
        if (StringUtils.isBlank(propValue)) {
            throw new SeaTunnelException(
                    CommonError.convertToConnectorPropsBlankError("Paimon", propKey));
        }
        return propValue;
    }

    protected static List<String> stringToList(String value, String regex) {
        if (value == null || value.isEmpty()) {
            return ImmutableList.of();
        }
        return Arrays.stream(value.split(regex)).map(String::trim).collect(toList());
    }
}
