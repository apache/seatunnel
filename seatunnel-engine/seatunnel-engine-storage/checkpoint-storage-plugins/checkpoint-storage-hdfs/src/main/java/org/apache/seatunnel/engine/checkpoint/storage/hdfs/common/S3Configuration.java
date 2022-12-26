/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.checkpoint.storage.hdfs.common;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * S3Configuration
 * we just support s3n and s3a protocol.
 * some hadoop low version not support s3a, if you want to use s3a, you should check your hadoop version first.
 * <p>
 * bucket is required, and the default schema is s3n
 * we used the bucket name to get the protocol,if you used s3a, this bucket name must be s3a://bucket, if you used s3n, this bucket name must be s3n://bucket
 * <p>
 * other configuration is optional, if you need to set other configuration, you can set it in the config
 * and the parameter name is the same as the hadoop configuration.
 * <p>
 * eg: if you want to set the endpoint, you can set it in the config like this: config.put("fs.s3a.endpoint", "http://),
 * the prefix is fs.s3a and must be the same as the hadoop configuration
 * <p>
 * more information about the configuration, please refer to the official website:
 * https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html
 */
public class S3Configuration extends AbstractConfiguration {

    /**************** S3 required keys ***************/
    public static final String S3_BUCKET_KEY = "s3.bucket";


    /* S3 constants */
    private static final String HDFS_S3N_IMPL = "org.apache.hadoop.fs.s3native.NativeS3FileSystem";
    private static final String HDFS_S3A_IMPL = "org.apache.hadoop.fs.s3a.S3AFileSystem";
    private static final String S3A_PROTOCOL = "s3a";
    private static final String DEFAULT_PROTOCOL = "s3n";
    private static final String S3_FORMAT_KEY = "fs.%s.%s";
    private static final String SPLIT_CHAR = ".";
    private static final String FS_KEY = "fs.";

    @Override
    public Configuration buildConfiguration(Map<String, String> config) {
        checkConfiguration(config, S3_BUCKET_KEY);
        String protocol = DEFAULT_PROTOCOL;
        if (config.get(S3_BUCKET_KEY).startsWith(S3A_PROTOCOL)) {
            protocol = S3A_PROTOCOL;
        }
        String fsImpl = protocol.equals(S3A_PROTOCOL) ? HDFS_S3A_IMPL : HDFS_S3N_IMPL;
        Configuration hadoopConf = new Configuration();
        hadoopConf.set(FS_DEFAULT_NAME_KEY, config.get(S3_BUCKET_KEY));
        hadoopConf.set(formatKey(protocol, HDFS_IMPL_KEY), fsImpl);
        setExtraConfiguration(hadoopConf, config, FS_KEY + protocol + SPLIT_CHAR);
        return hadoopConf;
    }

    private String formatKey(String protocol, String key) {
        return String.format(S3_FORMAT_KEY, protocol, key);
    }

}
