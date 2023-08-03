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

package org.apache.seatunnel.connectors.seatunnel.file.cos.config;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.apache.hadoop.fs.CosNConfigKeys;

import java.util.HashMap;

public class CosConf extends HadoopConf {
    private static final String HDFS_IMPL = "org.apache.hadoop.fs.CosFileSystem";
    private static final String SCHEMA = "cosn";

    @Override
    public String getFsHdfsImpl() {
        return HDFS_IMPL;
    }

    @Override
    public String getSchema() {
        return SCHEMA;
    }

    public CosConf(String hdfsNameKey) {
        super(hdfsNameKey);
    }

    public static HadoopConf buildWithConfig(Config config) {
        HadoopConf hadoopConf = new CosConf(config.getString(CosConfig.BUCKET.key()));
        HashMap<String, String> cosOptions = new HashMap<>();
        cosOptions.put(
                CosNConfigKeys.COSN_USERINFO_SECRET_ID_KEY,
                config.getString(CosConfig.SECRET_ID.key()));
        cosOptions.put(
                CosNConfigKeys.COSN_USERINFO_SECRET_KEY_KEY,
                config.getString(CosConfig.SECRET_KEY.key()));
        cosOptions.put(CosNConfigKeys.COSN_REGION_KEY, config.getString(CosConfig.REGION.key()));
        hadoopConf.setExtraOptions(cosOptions);
        return hadoopConf;
    }
}
