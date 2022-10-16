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

package org.apache.seatunnel.connectors.file.ftp.config;

import org.apache.seatunnel.connectors.file.config.HadoopConf;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.HashMap;

public class FtpConf extends HadoopConf {
    protected String fsHdfsImpl = "org.apache.seatunnel.connectors.seatunnel.file.ftp.system.SeaTunnelFTPFileSystem";

    private FtpConf(String hdfsNameKey) {
        super(hdfsNameKey);
    }

    @Override
    public String getFsHdfsImpl() {
        return fsHdfsImpl;
    }

    public static HadoopConf buildWithConfig(Config config) {
        String host = config.getString(FtpConfig.FTP_HOST);
        int port = config.getInt(FtpConfig.FTP_PORT);
        String defaultFS = String.format("ftp://%s:%s", host, port);
        HadoopConf hadoopConf = new FtpConf(defaultFS);
        HashMap<String, String> ftpOptions = new HashMap<>();
        ftpOptions.put("fs.ftp.user." + host, config.getString(FtpConfig.FTP_USERNAME));
        ftpOptions.put("fs.ftp.password." + host, config.getString(FtpConfig.FTP_PASSWORD));
        ftpOptions.put("fs.ftp.impl", hadoopConf.getFsHdfsImpl());
        hadoopConf.setExtraOptions(ftpOptions);
        return hadoopConf;
    }
}
