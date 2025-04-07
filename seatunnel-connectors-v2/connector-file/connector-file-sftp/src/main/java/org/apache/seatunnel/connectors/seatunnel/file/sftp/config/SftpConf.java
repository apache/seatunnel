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

package org.apache.seatunnel.connectors.seatunnel.file.sftp.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import java.util.HashMap;

public class SftpConf extends HadoopConf {
    private static final String HDFS_IMPL =
            "org.apache.seatunnel.connectors.seatunnel.file.sftp.system.SFTPFileSystem";
    private static final String SCHEMA = "sftp";

    private SftpConf(String hdfsNameKey) {
        super(hdfsNameKey);
    }

    @Override
    public String getFsHdfsImpl() {
        return HDFS_IMPL;
    }

    @Override
    public String getSchema() {
        return SCHEMA;
    }

    public static HadoopConf buildWithConfig(ReadonlyConfig config) {
        String host = config.get(SftpConfigOptions.SFTP_HOST);
        int port = config.get(SftpConfigOptions.SFTP_PORT);
        String defaultFS = String.format("sftp://%s:%s", host, port);
        HadoopConf hadoopConf = new SftpConf(defaultFS);
        HashMap<String, String> sftpOptions = new HashMap<>();
        sftpOptions.put("fs.sftp.user." + host, config.get(SftpConfigOptions.SFTP_USER));
        sftpOptions.put(
                "fs.sftp.password." + host + "." + config.get(SftpConfigOptions.SFTP_USER),
                config.get(SftpConfigOptions.SFTP_PASSWORD));
        hadoopConf.setExtraOptions(sftpOptions);
        return hadoopConf;
    }
}
