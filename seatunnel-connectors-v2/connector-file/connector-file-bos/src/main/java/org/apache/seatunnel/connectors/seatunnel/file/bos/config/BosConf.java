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

package org.apache.seatunnel.connectors.seatunnel.file.bos.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import java.util.HashMap;

public class BosConf extends HadoopConf {
    private static final String HDFS_IMPL = "org.apache.hadoop.fs.bos.BaiduBosFileSystem";
    private static final String SCHEMA = "bos";
    private static final String ACCESS_KEY = "fs.bos.access.key";
    private static final String SECRET_KEY = "fs.bos.secret.access.key";
    private static final String ENDPOINT = "fs.bos.endpoint";
    private static final String BUCKET_HIERARCHY = "fs.bos.bucket.hierarchy";

    @Override
    public String getFsHdfsImpl() {
        return HDFS_IMPL;
    }

    @Override
    public String getSchema() {
        return SCHEMA;
    }

    public BosConf(String hdfsNameKey) {
        super(hdfsNameKey);
    }

    public static HadoopConf buildWithReadonlyConfig(ReadonlyConfig readonlyConfig) {
        HadoopConf hadoopConf = new BosConf(readonlyConfig.get(BosFileBaseOptions.BUCKET));
        HashMap<String, String> bosOptions = new HashMap<>();
        bosOptions.put(ACCESS_KEY, readonlyConfig.get(BosFileBaseOptions.ACCESS_KEY));
        bosOptions.put(SECRET_KEY, readonlyConfig.get(BosFileBaseOptions.SECRET_KEY));
        bosOptions.put(ENDPOINT, readonlyConfig.get(BosFileBaseOptions.ENDPOINT));
        bosOptions.put(BUCKET_HIERARCHY, "false");
        hadoopConf.setExtraOptions(bosOptions);
        return hadoopConf;
    }
}
