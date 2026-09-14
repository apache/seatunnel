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

package org.apache.seatunnel.connectors.seatunnel.hive.storage;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.bos.config.BosConf;
import org.apache.seatunnel.connectors.seatunnel.file.bos.config.BosFileBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.apache.hadoop.conf.Configuration;

import java.util.Map;

/**
 * Builds Hadoop configuration for Hive tables stored on BOS ({@code bos://}).
 *
 * <p>Loads bucket and BOS credentials from {@code hive-site.xml}/{@code core-site.xml} or {@code
 * hive.hadoop.conf}, then delegates to {@link BosConf} for {@link HadoopConf} used by Hive
 * source/sink file access.
 */
public class BOSStorage extends AbstractStorage {

    private static final String FS_BOS_ACCESS_KEY = "fs.bos.access.key";
    private static final String FS_BOS_SECRET_KEY = "fs.bos.secret.access.key";
    private static final String FS_BOS_ENDPOINT = "fs.bos.endpoint";

    @Override
    public HadoopConf buildHadoopConfWithReadOnlyConfig(ReadonlyConfig readonlyConfig) {
        Configuration configuration = loadHiveBaseHadoopConfig(readonlyConfig);
        Config config = fillBucket(readonlyConfig, configuration);
        config =
                config.withValue(
                        BosFileBaseOptions.ACCESS_KEY.key(),
                        ConfigValueFactory.fromAnyRef(
                                getConfigurationValue(
                                        configuration,
                                        BosFileBaseOptions.ACCESS_KEY.key(),
                                        FS_BOS_ACCESS_KEY)));
        config =
                config.withValue(
                        BosFileBaseOptions.SECRET_KEY.key(),
                        ConfigValueFactory.fromAnyRef(
                                getConfigurationValue(
                                        configuration,
                                        BosFileBaseOptions.SECRET_KEY.key(),
                                        FS_BOS_SECRET_KEY)));
        config =
                config.withValue(
                        BosFileBaseOptions.ENDPOINT.key(),
                        ConfigValueFactory.fromAnyRef(
                                getConfigurationValue(
                                        configuration,
                                        BosFileBaseOptions.ENDPOINT.key(),
                                        FS_BOS_ENDPOINT)));
        HadoopConf hadoopConf = BosConf.buildWithConfig(config);
        Map<String, String> propsInConfiguration =
                configuration.getPropsWithPrefix(StringUtils.EMPTY);
        hadoopConf.setExtraOptions(propsInConfiguration);
        return hadoopConf;
    }

    private static String getConfigurationValue(
            Configuration configuration, String optionKey, String hadoopKey) {
        String value = configuration.get(optionKey);
        if (StringUtils.isBlank(value)) {
            value = configuration.get(hadoopKey);
        }
        return value;
    }
}
