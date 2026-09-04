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

package org.apache.seatunnel.connectors.seatunnel.file.gcs.config;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/** Hadoop filesystem configuration for Google Cloud Storage. */
public class GcsHadoopConf extends HadoopConf {

    static final String GCS_FILESYSTEM_IMPLEMENTATION =
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem";
    static final String GCS_SERVICE_ACCOUNT_KEY_FILE = "fs.gs.auth.service.account.json.keyfile";
    private static final String SCHEMA = "gs";

    private GcsHadoopConf(String bucket) {
        super(bucket);
    }

    /** Builds a GCS Hadoop configuration from connector options. */
    public static HadoopConf buildWithReadonlyConfig(ReadonlyConfig config) {
        String bucket = config.get(GcsFileBaseOptions.BUCKET);
        validateBucket(bucket);

        Map<String, String> properties = new HashMap<>();
        config.getOptional(GcsFileBaseOptions.GCS_PROPERTIES).ifPresent(properties::putAll);

        config.getOptional(GcsFileBaseOptions.SERVICE_ACCOUNT_KEY_FILE)
                .ifPresent(keyFile -> configureServiceAccount(properties, keyFile));

        GcsHadoopConf hadoopConf = new GcsHadoopConf(bucket);
        hadoopConf.setExtraOptions(properties);
        return hadoopConf;
    }

    @Override
    public String getFsHdfsImpl() {
        return GCS_FILESYSTEM_IMPLEMENTATION;
    }

    @Override
    public String getSchema() {
        return SCHEMA;
    }

    private static void validateBucket(String bucket) {
        if (StringUtils.isBlank(bucket)) {
            throw invalidBucket(bucket);
        }
        try {
            URI uri = new URI(bucket);
            String path = uri.getPath();
            if (!SCHEMA.equals(uri.getScheme())
                    || StringUtils.isBlank(uri.getAuthority())
                    || uri.getUserInfo() != null
                    || uri.getPort() != -1
                    || uri.getQuery() != null
                    || uri.getFragment() != null
                    || (StringUtils.isNotEmpty(path) && !"/".equals(path))) {
                throw invalidBucket(bucket);
            }
        } catch (URISyntaxException e) {
            throw invalidBucket(bucket);
        }
    }

    private static IllegalArgumentException invalidBucket(String bucket) {
        return new IllegalArgumentException(
                String.format(
                        "The GCS bucket must be a bucket URI such as 'gs://my-bucket', but was '%s'. "
                                + "Configure object paths with the 'path' option.",
                        bucket));
    }

    private static void configureServiceAccount(
            Map<String, String> properties, String serviceAccountKeyFile) {
        if (StringUtils.isBlank(serviceAccountKeyFile)) {
            throw new IllegalArgumentException(
                    "The GCS service_account_key_file option must not be blank");
        }
        properties.put(GCS_SERVICE_ACCOUNT_KEY_FILE, serviceAccountKeyFile);
    }
}
