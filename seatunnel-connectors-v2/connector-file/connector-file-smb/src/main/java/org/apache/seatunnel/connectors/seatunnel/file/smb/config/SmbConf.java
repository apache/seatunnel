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

package org.apache.seatunnel.connectors.seatunnel.file.smb.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.smb.system.SmbFileSystem;

import java.util.HashMap;

/**
 * SMB-specific Hadoop configuration. Translates user-facing connector options (host, port, user,
 * password, domain, share) into the {@code fs.smb.*} properties that {@link
 * org.apache.seatunnel.connectors.seatunnel.file.smb.system.SmbFileSystem} reads at init time.
 * Validates that host, user, and share are present at construction time so config errors surface
 * immediately rather than at first I/O.
 */
public class SmbConf extends HadoopConf {
    private static final String HDFS_IMPL =
            "org.apache.seatunnel.connectors.seatunnel.file.smb.system.SmbFileSystem";
    private static final String SCHEMA = "smb";

    private SmbConf(String hdfsNameKey) {
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
        String host = config.get(SmbFileBaseOptions.SMB_HOST);
        String user = config.get(SmbFileBaseOptions.SMB_USER);
        String share = config.get(SmbFileBaseOptions.SMB_SHARE);
        if (host == null || host.isEmpty()) {
            throw new IllegalArgumentException(
                    "SMB 'host' is required but was not provided. "
                            + "Please set the 'host' option in your connector configuration.");
        }
        if (user == null || user.isEmpty()) {
            throw new IllegalArgumentException(
                    "SMB 'user' is required but was not provided. "
                            + "Please set the 'user' option in your connector configuration.");
        }
        if (share == null || share.isEmpty()) {
            throw new IllegalArgumentException(
                    "SMB 'share' is required but was not provided. "
                            + "Please set the 'share' option in your connector configuration.");
        }
        int port = config.get(SmbFileBaseOptions.SMB_PORT);
        String defaultFS = String.format("smb://%s:%s", host, port);
        HadoopConf hadoopConf = new SmbConf(defaultFS);
        HashMap<String, String> smbOptions = new HashMap<>();
        smbOptions.put(SmbFileSystem.FS_SMB_HOST, host);
        smbOptions.put(SmbFileSystem.FS_SMB_PORT, String.valueOf(port));
        smbOptions.put(SmbFileSystem.FS_SMB_USER, user);
        config.getOptional(SmbFileBaseOptions.SMB_PASSWORD)
                .ifPresent(password -> smbOptions.put(SmbFileSystem.FS_SMB_PASSWORD, password));
        smbOptions.put(SmbFileSystem.FS_SMB_DOMAIN, config.get(SmbFileBaseOptions.SMB_DOMAIN));
        smbOptions.put(SmbFileSystem.FS_SMB_SHARE, share);
        hadoopConf.setExtraOptions(smbOptions);
        return hadoopConf;
    }
}
