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

package org.apache.seatunnel.connectors.seatunnel.file.s3.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import java.util.HashMap;
import java.util.Map;

public class S3HadoopConf extends HadoopConf {
    private static final String HDFS_S3N_IMPL = "org.apache.hadoop.fs.s3native.NativeS3FileSystem";
    private static final String HDFS_S3A_IMPL = "org.apache.hadoop.fs.s3a.S3AFileSystem";
    protected static final String S3A_SCHEMA = "s3a";
    protected static final String DEFAULT_SCHEMA = "s3n";
    private String schema = DEFAULT_SCHEMA;

    @Override
    public String getFsHdfsImpl() {
        return switchHdfsImpl();
    }

    @Override
    public String getSchema() {
        return this.schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public S3HadoopConf(String hdfsNameKey) {
        super(hdfsNameKey);
    }

    public static HadoopConf buildWithReadOnlyConfig(ReadonlyConfig config) {

        String bucketName = config.get(S3FileBaseOptions.S3_BUCKET);
        S3HadoopConf hadoopConf = new S3HadoopConf(bucketName);
        if (bucketName.startsWith(S3A_SCHEMA)) {
            hadoopConf.setSchema(S3A_SCHEMA);
        }
        HashMap<String, String> s3Options = new HashMap<>();
        hadoopConf.putS3SK(s3Options, config);
        if (config.getOptional(S3FileBaseOptions.S3_PROPERTIES).isPresent()) {
            config.get(S3FileBaseOptions.S3_PROPERTIES)
                    .forEach((key, value) -> s3Options.put(key, String.valueOf(value)));
        }

        String credentialsProvider = config.get(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER);
        checkCredentialsProviderOnClasspath(credentialsProvider);
        s3Options.put(S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER.key(), credentialsProvider);
        s3Options.put(
                S3FileBaseOptions.FS_S3A_ENDPOINT.key(),
                config.get(S3FileBaseOptions.FS_S3A_ENDPOINT));
        hadoopConf.setExtraOptions(s3Options);
        return hadoopConf;
    }

    protected String switchHdfsImpl() {
        switch (this.schema) {
            case S3A_SCHEMA:
                return HDFS_S3A_IMPL;
            default:
                return HDFS_S3N_IMPL;
        }
    }

    private void putS3SK(Map<String, String> s3Options, ReadonlyConfig config) {
        if (!config.getOptional(S3FileBaseOptions.S3_ACCESS_KEY).isPresent()
                && !config.getOptional(S3FileBaseOptions.S3_SECRET_KEY).isPresent()) {
            return;
        }
        String accessKey = config.get(S3FileBaseOptions.S3_ACCESS_KEY);
        String secretKey = config.get(S3FileBaseOptions.S3_SECRET_KEY);
        if (S3A_SCHEMA.equals(this.schema)) {
            s3Options.put("fs.s3a.access.key", accessKey);
            s3Options.put("fs.s3a.secret.key", secretKey);
            return;
        }
        // default s3n
        s3Options.put("fs.s3n.awsAccessKeyId", accessKey);
        s3Options.put("fs.s3n.awsSecretAccessKey", secretKey);
    }

    /**
     * Verifies that the configured S3A credentials provider class is present on the classpath. Any
     * fully-qualified provider class is accepted, but a missing class is reported eagerly with an
     * actionable message instead of surfacing later as an obscure Hadoop initialization failure.
     *
     * <p>The lookup tries the thread context classloader first, then falls back to the classloader
     * that loaded this connector. Under the Zeta plugin classloader the context classloader may be
     * {@code null} or unrelated to the connector, so the fallback ensures a provider bundled with
     * the connector (including the default {@link
     * S3FileBaseOptions#INSTANCE_PROFILE_CREDENTIALS_PROVIDER}) is not rejected by mistake.
     *
     * @param credentialsProvider the fully-qualified credentials provider class name
     */
    private static void checkCredentialsProviderOnClasspath(String credentialsProvider) {
        ClassLoader[] classLoaders = {
            Thread.currentThread().getContextClassLoader(), S3HadoopConf.class.getClassLoader()
        };
        for (ClassLoader classLoader : classLoaders) {
            if (classLoader == null) {
                continue;
            }
            try {
                Class.forName(credentialsProvider, false, classLoader);
                return;
            } catch (ClassNotFoundException ignored) {
                // Try the next classloader before failing.
            }
        }
        throw new IllegalArgumentException(
                String.format(
                        "The S3A credentials provider class '%s' is not found on the classpath. "
                                + "Please check the value of '%s' and make sure the provider class "
                                + "is available in the runtime classpath (for example under "
                                + "${SEATUNNEL_HOME}/lib).",
                        credentialsProvider, S3FileBaseOptions.S3A_AWS_CREDENTIALS_PROVIDER.key()));
    }
}
