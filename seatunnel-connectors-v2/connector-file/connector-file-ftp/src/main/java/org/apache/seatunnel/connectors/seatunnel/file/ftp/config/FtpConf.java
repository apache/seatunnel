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

import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.HashMap;

public class FtpConf extends HadoopConf {
    private static final String HDFS_IMPL = "org.apache.seatunnel.connectors.seatunnel.file.ftp.system.SeaTunnelFTPFileSystem";
    private static final String SCHEMA = "ftp";

    private FtpConf(String hdfsNameKey) {
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

    public static HadoopConf buildWithConfig(Config config) {
        String host = config.getString(FtpConfig.FTP_HOST.key());
        int port = config.getInt(FtpConfig.FTP_PORT.key());
        String defaultFS = String.format("ftp://%s:%s", host, port);
        HadoopConf hadoopConf = new FtpConf(defaultFS);
        HashMap<String, String> ftpOptions = new HashMap<>();
        ftpOptions.put("fs.ftp.user." + host, config.getString(FtpConfig.FTP_USERNAME.key()));
        ftpOptions.put("fs.ftp.password." + host, config.getString(FtpConfig.FTP_PASSWORD.key()));
        hadoopConf.setExtraOptions(ftpOptions);
        return hadoopConf;
    }
}
