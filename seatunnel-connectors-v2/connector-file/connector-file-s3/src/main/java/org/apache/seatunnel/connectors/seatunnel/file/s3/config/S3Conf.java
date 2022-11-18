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

import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.HashMap;

public class S3Conf extends HadoopConf {
    private static final String HDFS_IMPL = "org.apache.hadoop.fs.s3native.NativeS3FileSystem";
    private static final String SCHEMA = "s3n";

    @Override
    public String getFsHdfsImpl() {
        return HDFS_IMPL;
    }

    @Override
    public String getSchema() {
        return SCHEMA;
    }

    private S3Conf(String hdfsNameKey) {
        super(hdfsNameKey);
    }

    public static HadoopConf buildWithConfig(Config config) {
        HadoopConf hadoopConf = new S3Conf(config.getString(S3Config.S3_BUCKET.key()));
        HashMap<String, String> s3Options = new HashMap<>();
        s3Options.put("fs.s3n.awsAccessKeyId", config.getString(S3Config.S3_ACCESS_KEY.key()));
        s3Options.put("fs.s3n.awsSecretAccessKey", config.getString(S3Config.S3_SECRET_KEY.key()));
        hadoopConf.setExtraOptions(s3Options);
        return hadoopConf;
    }
}
