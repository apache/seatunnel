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

package org.apache.seatunnel.connectors.seatunnel.file.adls.config;

import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/** Builds and validates the isolated ABFS runtime configuration used by ADLS. */
public final class ADLSRuntimeCompatibility {
    public static final String SECURE_ABFS_SCHEME = "abfss";
    public static final String SECURE_ABFS_IMPLEMENTATION =
            "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem";
    private static final String DEFAULT_ENDPOINT_SUFFIX = "dfs.core.windows.net";
    private static final String DEFAULT_AUTHORITY_HOST = "https://login.microsoftonline.com";
    private static final String CLIENT_CREDENTIALS_PROVIDER =
            "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider";

    private ADLSRuntimeCompatibility() {}

    /**
     * Creates the minimum secure ABFS configuration without contacting Azure.
     *
     * @param accountName storage account DNS label
     * @param container filesystem/container name
     * @return configuration suitable for a Hadoop FileSystem lookup
     */
    public static Configuration newConfiguration(String accountName, String container) {
        Configuration configuration = new Configuration(false);
        configuration.set(
                "fs.defaultFS", secureAbfsUri(accountName, container, DEFAULT_ENDPOINT_SUFFIX));
        configuration.set("fs." + SECURE_ABFS_SCHEME + ".impl", SECURE_ABFS_IMPLEMENTATION);
        configuration.setBoolean("fs." + SECURE_ABFS_SCHEME + ".impl.disable.cache", true);
        return configuration;
    }

    /** Builds the secure ABFS URI used by the connector and Hadoop configuration. */
    public static String secureAbfsUri(
            String accountName, String container, String endpointSuffix) {
        validateDnsLabel(accountName, "accountName");
        validateDnsLabel(container, "container");
        validateEndpointSuffix(endpointSuffix);
        return SECURE_ABFS_SCHEME + "://" + container + "@" + accountName + "." + endpointSuffix;
    }

    /** Fails fast when the ABFS implementation is absent from the runtime classpath. */
    public static void validateDriverAvailable() {
        try {
            Class.forName(SECURE_ABFS_IMPLEMENTATION, false, classLoader());
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(
                    "ABFS driver is unavailable. Install the approved Hadoop Azure runtime "
                            + "containing "
                            + SECURE_ABFS_IMPLEMENTATION,
                    e);
        }
    }

    /** Returns the account-scoped Shared Key settings expected by ABFS. */
    public static void configureSharedKey(
            Configuration configuration, String accountName, String accountKey) {
        sharedKeyOptions(accountName, DEFAULT_ENDPOINT_SUFFIX, accountKey)
                .forEach(configuration::set);
    }

    /** Builds the account-scoped Shared Key settings expected by ABFS. */
    public static Map<String, String> sharedKeyOptions(
            String accountName, String endpointSuffix, String accountKey) {
        String host = accountHost(accountName, endpointSuffix);
        requireNonBlank(accountKey, "accountKey");
        Map<String, String> options = new HashMap<>();
        options.put("fs.azure.account.auth.type." + host, "SharedKey");
        options.put("fs.azure.account.key." + host, accountKey);
        return options;
    }

    /** Returns the account-scoped OAuth client-credentials settings expected by ABFS. */
    public static void configureClientCredentials(
            Configuration configuration,
            String accountName,
            String tenantId,
            String clientId,
            String clientSecret) {
        clientCredentialsOptions(
                        accountName,
                        DEFAULT_ENDPOINT_SUFFIX,
                        DEFAULT_AUTHORITY_HOST,
                        tenantId,
                        clientId,
                        clientSecret)
                .forEach(configuration::set);
    }

    /** Builds the account-scoped OAuth client-credentials settings expected by ABFS. */
    public static Map<String, String> clientCredentialsOptions(
            String accountName,
            String endpointSuffix,
            String authorityHost,
            String tenantId,
            String clientId,
            String clientSecret) {
        String host = accountHost(accountName, endpointSuffix);
        String normalizedAuthorityHost = normalizeAuthorityHost(authorityHost);
        requireNonBlank(tenantId, "tenantId");
        requireNonBlank(clientId, "clientId");
        requireNonBlank(clientSecret, "clientSecret");
        Map<String, String> options = new HashMap<>();
        options.put("fs.azure.account.auth.type." + host, "OAuth");
        options.put("fs.azure.account.oauth.provider.type." + host, CLIENT_CREDENTIALS_PROVIDER);
        options.put("fs.azure.account.oauth2.client.id." + host, clientId);
        options.put("fs.azure.account.oauth2.client.secret." + host, clientSecret);
        options.put(
                "fs.azure.account.oauth2.client.endpoint." + host,
                normalizedAuthorityHost + "/" + tenantId + "/oauth2/token");
        return options;
    }

    private static ClassLoader classLoader() {
        ClassLoader context = Thread.currentThread().getContextClassLoader();
        return context == null ? ADLSRuntimeCompatibility.class.getClassLoader() : context;
    }

    private static void validateDnsLabel(String value, String name) {
        requireNonBlank(value, name);
        try {
            new URI("abfss://" + value + "@example.dfs.core.windows.net/");
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(name + " is not a valid ABFS label", e);
        }
        if (!value.matches("[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?")) {
            throw new IllegalArgumentException(name + " must be a lowercase DNS label");
        }
    }

    private static String accountHost(String accountName, String endpointSuffix) {
        validateDnsLabel(accountName, "accountName");
        validateEndpointSuffix(endpointSuffix);
        return accountName + "." + endpointSuffix;
    }

    private static void validateEndpointSuffix(String endpointSuffix) {
        requireNonBlank(endpointSuffix, "endpointSuffix");
        if (!endpointSuffix.matches("[a-zA-Z0-9](?:[a-zA-Z0-9.-]*[a-zA-Z0-9])?")) {
            throw new IllegalArgumentException("endpointSuffix must be a DNS suffix");
        }
    }

    private static String normalizeAuthorityHost(String authorityHost) {
        requireNonBlank(authorityHost, "authorityHost");
        String normalized = authorityHost;
        while (normalized.endsWith("/")) {
            normalized = normalized.substring(0, normalized.length() - 1);
        }
        return normalized;
    }

    private static void requireNonBlank(String value, String name) {
        if (value == null || value.trim().isEmpty()) {
            throw new IllegalArgumentException(name + " must not be blank");
        }
    }
}
