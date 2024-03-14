/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.checkpoint.storage.hdfs.common;

import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.Map;

import static org.apache.hadoop.fs.FileSystem.FS_DEFAULT_NAME_KEY;

public class HdfsConfiguration extends AbstractConfiguration {

    /** hdfs uri is required */
    private static final String HDFS_DEF_FS_NAME = "fs.defaultFS";
    /** hdfs kerberos principal( is optional) */
    private static final String KERBEROS_PRINCIPAL = "kerberosPrincipal";

    private static final String KERBEROS_KEYTAB_FILE_PATH = "kerberosKeytabFilePath";
    private static final String HADOOP_SECURITY_AUTHENTICATION_KEY =
            "hadoop.security.authentication";

    private static final String KERBEROS_KEY = "kerberos";

    /** ********* Hdfs constants ************* */
    private static final String HDFS_IMPL = "org.apache.hadoop.hdfs.DistributedFileSystem";

    private static final String HDFS_IMPL_KEY = "fs.hdfs.impl";

    private static final String SEATUNNEL_HADOOP_PREFIX = "seatunnel.hadoop.";

    @Override
    public Configuration buildConfiguration(Map<String, String> config)
            throws CheckpointStorageException {
        checkConfiguration(config, HDFS_DEF_FS_NAME);
        Configuration hadoopConf = new Configuration();
        if (config.containsKey(HDFS_DEF_FS_NAME)) {
            hadoopConf.set(HDFS_DEF_FS_NAME, config.get(HDFS_DEF_FS_NAME));
        }
        hadoopConf.set(HDFS_IMPL_KEY, HDFS_IMPL);
        hadoopConf.set(FS_DEFAULT_NAME_KEY, config.get(FS_DEFAULT_NAME_KEY));
        if (config.containsKey(KERBEROS_PRINCIPAL)
                && config.containsKey(KERBEROS_KEYTAB_FILE_PATH)) {
            String kerberosPrincipal = config.get(KERBEROS_PRINCIPAL);
            String kerberosKeytabFilePath = config.get(KERBEROS_KEYTAB_FILE_PATH);
            if (StringUtils.isNotBlank(kerberosPrincipal)
                    && StringUtils.isNotBlank(kerberosKeytabFilePath)) {
                hadoopConf.set(HADOOP_SECURITY_AUTHENTICATION_KEY, KERBEROS_KEY);
                authenticateKerberos(kerberosPrincipal, kerberosKeytabFilePath, hadoopConf);
            }
        }
        //  support other hdfs optional config keys
        config.entrySet().stream()
                .filter(entry -> entry.getKey().startsWith(SEATUNNEL_HADOOP_PREFIX))
                .forEach(
                        entry -> {
                            String key = entry.getKey().replace(SEATUNNEL_HADOOP_PREFIX, "");
                            String value = entry.getValue();
                            hadoopConf.set(key, value);
                        });
        return hadoopConf;
    }

    /**
     * Authenticate kerberos
     *
     * @param kerberosPrincipal kerberos principal
     * @param kerberosKeytabFilePath kerberos keytab file path
     * @param hdfsConf hdfs configuration
     * @throws CheckpointStorageException authentication exception
     */
    private void authenticateKerberos(
            String kerberosPrincipal, String kerberosKeytabFilePath, Configuration hdfsConf)
            throws CheckpointStorageException {
        UserGroupInformation.setConfiguration(hdfsConf);
        try {
            UserGroupInformation.loginUserFromKeytab(kerberosPrincipal, kerberosKeytabFilePath);
        } catch (IOException e) {
            throw new CheckpointStorageException(
                    "Failed to login user from keytab : "
                            + kerberosKeytabFilePath
                            + " and kerberos principal : "
                            + kerberosPrincipal,
                    e);
        }
    }
}
