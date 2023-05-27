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

public class OssConfiguration extends AbstractConfiguration {
    public static final String OSS_BUCKET_KEY = "oss.bucket";
    private static final String OSS_IMPL_KEY = "fs.oss.impl";
    private static final String HDFS_OSS_IMPL =
            "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem";
    private static final String OSS_KEY = "fs.oss.";

    @Override
    public Configuration buildConfiguration(Map<String, Object> config)
            throws IMapStorageException {
        checkConfiguration(config, OSS_BUCKET_KEY);
        Configuration hadoopConf = new Configuration();
        hadoopConf.set(FS_DEFAULT_NAME_KEY, String.valueOf(config.get(OSS_BUCKET_KEY)));
        hadoopConf.set(OSS_IMPL_KEY, HDFS_OSS_IMPL);
        setExtraConfiguration(hadoopConf, config, OSS_KEY);
        return hadoopConf;
    }
}
