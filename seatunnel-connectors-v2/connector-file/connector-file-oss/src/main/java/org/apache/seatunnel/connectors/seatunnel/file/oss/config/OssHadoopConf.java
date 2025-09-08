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

package org.apache.seatunnel.connectors.seatunnel.file.oss.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.apache.hadoop.fs.aliyun.oss.Constants;

import java.util.HashMap;

public class OssHadoopConf extends HadoopConf {
    private static final String HDFS_IMPL = "org.apache.hadoop.fs.aliyun.oss.AliyunOSSFileSystem";
    private static final String SCHEMA = "oss";

    @Override
    public String getFsHdfsImpl() {
        return HDFS_IMPL;
    }

    @Override
    public String getSchema() {
        return SCHEMA;
    }

    public OssHadoopConf(String hdfsNameKey) {
        super(hdfsNameKey);
    }

    public static HadoopConf buildWithConfig(ReadonlyConfig config) {
        HadoopConf hadoopConf = new OssHadoopConf(config.get(OssFileBaseOptions.BUCKET));
        HashMap<String, String> ossOptions = new HashMap<>();
        ossOptions.put(Constants.ACCESS_KEY_ID, config.get(OssFileBaseOptions.ACCESS_KEY));
        ossOptions.put(Constants.ACCESS_KEY_SECRET, config.get(OssFileBaseOptions.ACCESS_SECRET));
        ossOptions.put(Constants.ENDPOINT_KEY, config.get(OssFileBaseOptions.ENDPOINT));
        hadoopConf.setExtraOptions(ossOptions);
        return hadoopConf;
    }
}
