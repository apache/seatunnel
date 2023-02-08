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

import static org.apache.hadoop.fs.CommonConfigurationKeysPublic.FS_DEFAULT_NAME_DEFAULT;
import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

public class LocalConfiguration extends AbstractConfiguration {

    private static final String HDFS_LOCAL_IMPL = "org.apache.hadoop.fs.LocalFileSystem";
    private static final String HDFS_LOCAL_IMPL_KEY = "fs.file.impl";

    @Override
    public Configuration buildConfiguration(Map<String, String> config) {
        Configuration hadoopConf = new Configuration();
        hadoopConf.set(HDFS_LOCAL_IMPL_KEY, HDFS_LOCAL_IMPL);
        hadoopConf.set(FS_DEFAULT_NAME_KEY, config.getOrDefault(FS_DEFAULT_NAME_KEY, FS_DEFAULT_NAME_DEFAULT));
        return hadoopConf;
    }
}
