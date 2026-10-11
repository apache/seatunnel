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

package org.apache.seatunnel.engine.checkpoint.storage.hdfs.common;

import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

/**
 * Azure Blob Storage configuration for checkpoint storage, backed by the Hadoop WASB file system
 * ({@code hadoop-azure}), which must be placed in the lib directory together with its Azure Storage
 * SDK dependency.
 *
 * <p>{@code azure.container} is the container URI, {@code
 * wasbs://<container>@<account>.blob.core.windows.net} (recommended) or {@code wasb://...}. ADLS
 * Gen2 ({@code abfs://}, {@code abfss://}) is not supported by this storage type.
 *
 * <p>All {@code fs.azure.*} keys are passed through to hadoop-azure, e.g. the storage account key
 * {@code fs.azure.account.key.<account>.blob.core.windows.net} or a container SAS token {@code
 * fs.azure.sas.<container>.<account>.blob.core.windows.net}. Their values are never logged.
 */
public class AzureConfiguration extends AbstractConfiguration {

    /** ************** Azure required keys ************** */
    public static final String AZURE_CONTAINER_KEY = "azure.container";

    /* Azure constants */
    private static final String WASB_SCHEME = "wasb";
    private static final String WASBS_SCHEME = "wasbs";
    private static final String WASB_IMPL_KEY = "fs.wasb.impl";
    private static final String WASBS_IMPL_KEY = "fs.wasbs.impl";
    private static final String HDFS_WASB_IMPL = "org.apache.hadoop.fs.azure.NativeAzureFileSystem";
    private static final String HDFS_WASBS_IMPL =
            "org.apache.hadoop.fs.azure.NativeAzureFileSystem$Secure";
    private static final String AZURE_KEY = "fs.azure.";
    private static final String CONTAINER_URI_EXAMPLE =
            "wasbs://<container>@<account>.blob.core.windows.net";

    @Override
    public Configuration buildConfiguration(Map<String, String> config) {
        checkConfiguration(config, AZURE_CONTAINER_KEY);
        String container = validateContainerUri(config.get(AZURE_CONTAINER_KEY));
        Configuration hadoopConf = new Configuration();
        hadoopConf.set(FS_DEFAULT_NAME_KEY, container);
        hadoopConf.set(WASB_IMPL_KEY, HDFS_WASB_IMPL);
        hadoopConf.set(WASBS_IMPL_KEY, HDFS_WASBS_IMPL);
        boolean disableCache =
                Boolean.parseBoolean(
                        config.getOrDefault(DISABLE_CACHE_KEY, DISABLE_CACHE_DEFAULT_VALUE));
        hadoopConf.setBoolean(String.format(COMMON_DISABLE_CACHE, WASB_IMPL_KEY), disableCache);
        hadoopConf.setBoolean(String.format(COMMON_DISABLE_CACHE, WASBS_IMPL_KEY), disableCache);
        setExtraConfiguration(hadoopConf, config, AZURE_KEY);
        return hadoopConf;
    }

    /**
     * Checks that the value is an Azure Blob Storage container URI and returns it trimmed.
     *
     * @param value the configured {@code azure.container} value
     * @return the container URI to use as the default file system
     */
    static String validateContainerUri(String value) {
        String trimmed = value.trim();
        URI uri;
        try {
            uri = new URI(trimmed);
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException(invalidContainerMessage(trimmed), e);
        }
        String scheme = uri.getScheme();
        if ("abfs".equalsIgnoreCase(scheme) || "abfss".equalsIgnoreCase(scheme)) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s does not support ADLS Gen2 (abfs:// or abfss://) URIs; only Azure"
                                    + " Blob Storage container URIs such as '%s' are supported,"
                                    + " but was '%s'",
                            AZURE_CONTAINER_KEY, CONTAINER_URI_EXAMPLE, trimmed));
        }
        if (!WASB_SCHEME.equalsIgnoreCase(scheme) && !WASBS_SCHEME.equalsIgnoreCase(scheme)) {
            throw new IllegalArgumentException(invalidContainerMessage(trimmed));
        }
        String authority = uri.getRawAuthority();
        int separator = authority == null ? -1 : authority.indexOf('@');
        if (separator <= 0 || separator == authority.length() - 1) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s must name both the container and the storage account, as in"
                                    + " '%s', but was '%s'",
                            AZURE_CONTAINER_KEY, CONTAINER_URI_EXAMPLE, trimmed));
        }
        return trimmed;
    }

    private static String invalidContainerMessage(String value) {
        return String.format(
                "%s must be an Azure Blob Storage container URI such as '%s' (or wasb://), but"
                        + " was '%s'",
                AZURE_CONTAINER_KEY, CONTAINER_URI_EXAMPLE, value);
    }
}
