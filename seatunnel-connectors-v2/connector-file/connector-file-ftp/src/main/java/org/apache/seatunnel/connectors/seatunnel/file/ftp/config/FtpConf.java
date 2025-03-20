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

package org.apache.seatunnel.connectors.seatunnel.file.ftp.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.ftp.system.FtpConnectionMode;

import java.util.HashMap;
import java.util.Optional;

public class FtpConf extends HadoopConf {
    private static final String HDFS_IMPL =
            "org.apache.seatunnel.connectors.seatunnel.file.ftp.system.SeaTunnelFTPFileSystem";
    private static final String SCHEMA = "ftp";

    public FtpConf(String hdfsNameKey) {
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
        String host = config.get(FtpConfigOptions.FTP_HOST);
        int port = config.get(FtpConfigOptions.FTP_PORT);
        String defaultFS = String.format("ftp://%s:%s", host, port);
        HadoopConf hadoopConf = new FtpConf(defaultFS);
        HashMap<String, String> ftpOptions = new HashMap<>();
        ftpOptions.put("fs.ftp.user." + host, config.get(FtpConfigOptions.FTP_USERNAME));
        ftpOptions.put("fs.ftp.password." + host, config.get(FtpConfigOptions.FTP_PASSWORD));
        Optional<FtpConnectionMode> optional =
                config.getOptional(FtpConfigOptions.FTP_CONNECTION_MODE);
        if (optional.isPresent()) {
            ftpOptions.put(
                    "fs.ftp.connection.mode",
                    config.get(FtpConfigOptions.FTP_CONNECTION_MODE).toString());
        }
        hadoopConf.setExtraOptions(ftpOptions);
        return hadoopConf;
    }
}
