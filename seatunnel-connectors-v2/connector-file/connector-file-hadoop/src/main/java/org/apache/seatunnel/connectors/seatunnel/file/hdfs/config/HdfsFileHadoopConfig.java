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

package org.apache.seatunnel.connectors.seatunnel.file.hdfs.config;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.config.CheckConfigUtil;
import org.apache.seatunnel.common.config.CheckResult;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileSystemType;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.exception.FileConnectorException;

public class HdfsFileHadoopConfig extends HadoopConf {
    public HdfsFileHadoopConfig(String hdfsNameKey) {
        super(hdfsNameKey);
    }

    public static HadoopConf buildWithConfig(ReadonlyConfig readonlyConfig) {
        CheckResult result =
                CheckConfigUtil.checkAllExists(
                        readonlyConfig.toConfig(),
                        FileBaseSourceOptions.FILE_PATH.key(),
                        FileBaseSourceOptions.FILE_FORMAT_TYPE.key(),
                        FileBaseSourceOptions.DEFAULT_FS.key());
        if (!result.isSuccess()) {
            throw new FileConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            FileSystemType.HDFS.getFileSystemPluginName(),
                            PluginType.SOURCE,
                            result.getMsg()));
        }
        HadoopConf hadoopConf =
                new HdfsFileHadoopConfig(readonlyConfig.get(FileBaseSourceOptions.DEFAULT_FS));

        if (readonlyConfig.getOptional(FileBaseSourceOptions.HDFS_SITE_PATH).isPresent()) {
            hadoopConf.setHdfsSitePath(readonlyConfig.get(FileBaseSourceOptions.HDFS_SITE_PATH));
        }

        if (readonlyConfig.getOptional(FileBaseSourceOptions.REMOTE_USER).isPresent()) {
            hadoopConf.setRemoteUser(readonlyConfig.get(FileBaseSourceOptions.REMOTE_USER));
        }

        if (readonlyConfig.getOptional(FileBaseSourceOptions.KRB5_PATH).isPresent()) {
            hadoopConf.setKrb5Path(readonlyConfig.get(FileBaseSourceOptions.KRB5_PATH));
        }

        if (readonlyConfig.getOptional(FileBaseSourceOptions.KERBEROS_PRINCIPAL).isPresent()) {
            hadoopConf.setKerberosPrincipal(
                    readonlyConfig.get(FileBaseSourceOptions.KERBEROS_PRINCIPAL));
        }

        if (readonlyConfig.getOptional(FileBaseSourceOptions.KERBEROS_KEYTAB_PATH).isPresent()) {
            hadoopConf.setKerberosKeytabPath(
                    readonlyConfig.get(FileBaseSourceOptions.KERBEROS_KEYTAB_PATH));
        }

        return hadoopConf;
    }
}
