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

package org.apache.seatunnel.engine.imap.storage.file.config;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

/**
 * Google Cloud Storage configuration for IMap persistence, backed by the Hadoop GCS connector
 * ({@code gcs-connector}), which must be placed in the lib directory.
 *
 * <p>All {@code fs.gs.*} keys are passed through to the connector. Without {@code
 * fs.gs.auth.service.account.json.keyfile} the connector falls back to Application Default
 * Credentials, which covers GKE Workload Identity.
 */
public class GcsConfiguration extends AbstractConfiguration {

    public static final String GCS_BUCKET_KEY = "gcs.bucket";

    private static final String GCS_SCHEME_PREFIX = "gs://";
    private static final String GCS_IMPL_KEY = "fs.gs.impl";
    private static final String HDFS_GCS_IMPL =
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem";
    private static final String GCS_KEY = "fs.gs.";

    @Override
    public Configuration buildConfiguration(Map<String, String> config) {
        checkConfiguration(config, GCS_BUCKET_KEY);
        String bucket = config.get(GCS_BUCKET_KEY);
        if (!bucket.startsWith(GCS_SCHEME_PREFIX)) {
            throw new IllegalArgumentException(
                    String.format(
                            "%s must be a GCS bucket URI such as 'gs://my-bucket', but was '%s'",
                            GCS_BUCKET_KEY, bucket));
        }
        Configuration hadoopConf = new Configuration();
        hadoopConf.set(FS_DEFAULT_NAME_KEY, bucket);
        hadoopConf.set(GCS_IMPL_KEY, HDFS_GCS_IMPL);
        // Unlike the checkpoint storage GcsConfiguration, the FileSystem cache is intentionally
        // left at the Hadoop default, matching the IMap OSS and S3 configurations.
        setExtraConfiguration(hadoopConf, config, GCS_KEY);
        return hadoopConf;
    }
}
