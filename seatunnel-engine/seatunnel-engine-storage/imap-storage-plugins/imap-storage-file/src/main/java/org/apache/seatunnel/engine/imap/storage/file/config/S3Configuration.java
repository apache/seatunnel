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

package org.apache.seatunnel.engine.imap.storage.file.config;

import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

public class S3Configuration extends AbstractConfiguration {
    public static final String S3_BUCKET_KEY = "s3.bucket";
    private static final String HDFS_S3N_IMPL = "org.apache.hadoop.fs.s3native.NativeS3FileSystem";
    private static final String HDFS_S3A_IMPL = "org.apache.hadoop.fs.s3a.S3AFileSystem";
    private static final String S3A_PROTOCOL = "s3a";
    private static final String DEFAULT_PROTOCOL = "s3n";
    private static final String S3_FORMAT_KEY = "fs.%s.%s";
    private static final String SPLIT_CHAR = ".";
    private static final String FS_KEY = "fs.";

    @Override
    public Configuration buildConfiguration(Map<String, Object> config)
            throws IMapStorageException {
        checkConfiguration(config, S3_BUCKET_KEY);
        String protocol = DEFAULT_PROTOCOL;
        if (config.get(S3_BUCKET_KEY).toString().startsWith(S3A_PROTOCOL)) {
            protocol = S3A_PROTOCOL;
        }
        String fsImpl = protocol.equals(S3A_PROTOCOL) ? HDFS_S3A_IMPL : HDFS_S3N_IMPL;
        Configuration hadoopConf = new Configuration();
        hadoopConf.set(FS_DEFAULT_NAME_KEY, config.get(S3_BUCKET_KEY).toString());
        hadoopConf.set(formatKey(protocol, HDFS_IMPL_KEY), fsImpl);
        setExtraConfiguration(hadoopConf, config, FS_KEY + protocol + SPLIT_CHAR);
        return hadoopConf;
    }

    private String formatKey(String protocol, String key) {
        return String.format(S3_FORMAT_KEY, protocol, key);
    }
}
