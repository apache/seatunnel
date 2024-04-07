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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hive.exception.HiveConnectorException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Map;

public class HDFSStorage extends AbstractStorage {

    private String hiveSdLocation;

    public HDFSStorage(String hiveSdLocation) {
        this.hiveSdLocation = hiveSdLocation;
    }

    @Override
    public HadoopConf buildHadoopConfWithReadOnlyConfig(ReadonlyConfig readonlyConfig) {
        try {
            String path = new URI(hiveSdLocation).getPath();
            HadoopConf hadoopConf = new HadoopConf(hiveSdLocation.replace(path, StringUtils.EMPTY));
            Configuration configuration = loadHiveBaseHadoopConfig(readonlyConfig);
            Map<String, String> propsInConfiguration =
                    configuration.getPropsWithPrefix(StringUtils.EMPTY);
            hadoopConf.setExtraOptions(propsInConfiguration);
            return hadoopConf;
        } catch (URISyntaxException e) {
            String errorMsg =
                    String.format(
                            "Get hdfs namenode host from table location [%s] failed,"
                                    + "please check it",
                            hiveSdLocation);
            throw new HiveConnectorException(
                    HiveConnectorErrorCode.GET_HDFS_NAMENODE_HOST_FAILED, errorMsg, e);
        }
    }
}
