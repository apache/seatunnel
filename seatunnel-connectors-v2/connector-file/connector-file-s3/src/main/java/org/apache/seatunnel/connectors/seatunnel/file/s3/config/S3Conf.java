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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import java.util.HashMap;
import java.util.Map;

public class S3Conf extends HadoopConf {
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

    protected S3Conf(String hdfsNameKey) {
        super(hdfsNameKey);
    }

    public static HadoopConf buildWithConfig(Config config) {

        String bucketName = config.getString(S3ConfigOptions.S3_BUCKET.key());
        S3Conf hadoopConf = new S3Conf(bucketName);
        if (bucketName.startsWith(S3A_SCHEMA)) {
            hadoopConf.setSchema(S3A_SCHEMA);
        }
        HashMap<String, String> s3Options = new HashMap<>();
        hadoopConf.putS3SK(s3Options, config);
        if (CheckConfigUtil.isValidParam(config, S3ConfigOptions.S3_PROPERTIES.key())) {
            config.getObject(S3ConfigOptions.S3_PROPERTIES.key())
                    .forEach((key, value) -> s3Options.put(key, String.valueOf(value.unwrapped())));
        }

        s3Options.put(
                S3ConfigOptions.S3A_AWS_CREDENTIALS_PROVIDER.key(),
                config.getString(S3ConfigOptions.S3A_AWS_CREDENTIALS_PROVIDER.key()));
        s3Options.put(
                S3ConfigOptions.FS_S3A_ENDPOINT.key(),
                config.getString(S3ConfigOptions.FS_S3A_ENDPOINT.key()));
        hadoopConf.setExtraOptions(s3Options);
        return hadoopConf;
    }

    public static HadoopConf buildWithReadOnlyConfig(ReadonlyConfig readonlyConfig) {
        Config config = readonlyConfig.toConfig();
        return buildWithConfig(config);
    }

    protected String switchHdfsImpl() {
        switch (this.schema) {
            case S3A_SCHEMA:
                return HDFS_S3A_IMPL;
            default:
                return HDFS_S3N_IMPL;
        }
    }

    private void putS3SK(Map<String, String> s3Options, Config config) {
        if (!CheckConfigUtil.isValidParam(config, S3ConfigOptions.S3_ACCESS_KEY.key())
                && !CheckConfigUtil.isValidParam(config, S3ConfigOptions.S3_SECRET_KEY.key())) {
            return;
        }
        String accessKey = config.getString(S3ConfigOptions.S3_ACCESS_KEY.key());
        String secretKey = config.getString(S3ConfigOptions.S3_SECRET_KEY.key());
        if (S3A_SCHEMA.equals(this.schema)) {
            s3Options.put("fs.s3a.access.key", accessKey);
            s3Options.put("fs.s3a.secret.key", secretKey);
            return;
        }
        // default s3n
        s3Options.put("fs.s3n.awsAccessKeyId", accessKey);
        s3Options.put("fs.s3n.awsSecretAccessKey", secretKey);
    }
}
