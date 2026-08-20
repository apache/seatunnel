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

package org.apache.seatunnel.connectors.seatunnel.file.hdfs;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.config.HdfsFileHadoopConfig;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.sink.HdfsFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.hdfs.source.HdfsFileSourceFactory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.net.URI;
import java.util.HashMap;
import java.util.Map;

class HdfsFileFactoryTest {

    private static final String ABFS_DEFAULT_FS = "abfs://container@account.dfs.core.windows.net";
    private static final String ABFSS_DEFAULT_FS = "abfss://container@account.dfs.core.windows.net";

    @Test
    void optionRule() {
        OptionRule optionRule = (new HdfsFileSourceFactory()).optionRule();
        Assertions.assertNotNull(optionRule);
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.DISCOVERY_MODE));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.SCAN_INTERVAL));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.START_MODE));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.POST_SYNC_ACTION));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.BACKUP_PATH));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.RETENTION_MAX_AGE));
        Assertions.assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(FileBaseSourceOptions.RETENTION_CHECK_INTERVAL));
        Assertions.assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(FileBaseSourceOptions.UPDATE_COMPARE_PARALLELISM));
        Assertions.assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(FileBaseSourceOptions.UPDATE_COMPARE_BULK_THRESHOLD));
        Assertions.assertNotNull((new HdfsFileSinkFactory()).optionRule());
    }

    @Test
    void syncUpdateRequiresTargetPath() {
        OptionRule optionRule = (new HdfsFileSourceFactory()).optionRule();
        Map<String, Object> config = sourceConfig();
        config.put(FileBaseSourceOptions.SYNC_MODE.key(), "update");

        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(config, optionRule));

        config.put(FileBaseSourceOptions.TARGET_PATH.key(), "/target");
        Assertions.assertDoesNotThrow(() -> validate(config, optionRule));
    }

    @Test
    void postSyncActionValidation() {
        OptionRule optionRule = (new HdfsFileSourceFactory()).optionRule();
        Map<String, Object> noneConfig = sourceConfig();
        noneConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "none");
        Assertions.assertDoesNotThrow(() -> validate(noneConfig, optionRule));

        noneConfig.put(FileBaseSourceOptions.BACKUP_PATH.key(), "/backup");
        noneConfig.put(FileBaseSourceOptions.RETENTION_MAX_AGE.key(), "7D");
        noneConfig.put(FileBaseSourceOptions.RETENTION_CHECK_INTERVAL.key(), "1H");
        Assertions.assertDoesNotThrow(() -> validate(noneConfig, optionRule));

        Map<String, Object> backupConfig = sourceConfig();
        backupConfig.put(FileBaseSourceOptions.POST_SYNC_ACTION.key(), "backup");
        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(backupConfig, optionRule));

        backupConfig.put(FileBaseSourceOptions.BACKUP_PATH.key(), "/backup");
        Assertions.assertDoesNotThrow(() -> validate(backupConfig, optionRule));
    }

    @Test
    void supportsAbfsForSource() throws Exception {
        Map<String, Object> config = sourceConfig();
        config.put(FileBaseSourceOptions.FILE_FORMAT_TYPE.key(), "text");
        config.put(FileBaseSourceOptions.DEFAULT_FS.key(), ABFS_DEFAULT_FS);

        HadoopConf hadoopConf =
                HdfsFileHadoopConfig.buildWithConfig(ReadonlyConfig.fromMap(config));

        assertFileSystem(
                hadoopConf,
                "abfs",
                "org.apache.hadoop.fs.azurebfs.AzureBlobFileSystem",
                ABFS_DEFAULT_FS);
    }

    @Test
    void supportsAbfssForSink() throws Exception {
        Map<String, Object> config = new HashMap<>();
        config.put(FileBaseSinkOptions.FILE_PATH.key(), "/sink");
        config.put(FileBaseSinkOptions.DEFAULT_FS.key(), ABFSS_DEFAULT_FS);

        HadoopConf hadoopConf =
                new HdfsFileSinkFactory().initHadoopConf(ReadonlyConfig.fromMap(config));

        assertFileSystem(
                hadoopConf,
                "abfss",
                "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem",
                ABFSS_DEFAULT_FS);
    }

    @Test
    void preservesAbfsCredentialOptionsWithoutAllowingFilesystemOverrides() {
        HadoopConf hadoopConf = new HadoopConf(ABFSS_DEFAULT_FS);
        Map<String, String> extraOptions = new HashMap<>();
        String authTypeKey = "fs.azure.account.auth.type.account.dfs.core.windows.net";
        extraOptions.put(authTypeKey, "OAuth");
        extraOptions.put("fs.defaultFS", "hdfs://other-cluster");
        extraOptions.put("fs.abfss.impl", "example.InvalidFileSystem");
        hadoopConf.setExtraOptions(extraOptions);

        Configuration configuration = hadoopConf.toConfiguration();
        hadoopConf.setExtraOptionsForConfiguration(configuration);

        Assertions.assertEquals(ABFSS_DEFAULT_FS, configuration.get("fs.defaultFS"));
        Assertions.assertEquals(
                "org.apache.hadoop.fs.azurebfs.SecureAzureBlobFileSystem",
                configuration.get("fs.abfss.impl"));
        Assertions.assertEquals("OAuth", configuration.get(authTypeKey));
    }

    private static void assertFileSystem(
            HadoopConf hadoopConf, String scheme, String implementation, String defaultFs)
            throws Exception {
        Configuration configuration = hadoopConf.toConfiguration();

        Assertions.assertEquals(defaultFs, configuration.get("fs.defaultFS"));
        Assertions.assertEquals(implementation, configuration.get("fs." + scheme + ".impl"));
        Assertions.assertTrue(
                configuration.getBoolean("fs." + scheme + ".impl.disable.cache", false));
        Assertions.assertEquals(
                implementation, FileSystem.getFileSystemClass(scheme, configuration).getName());

        String account = "account.dfs.core.windows.net";
        configuration.set(
                "fs.azure.account.key." + account, "dGVzdC1rZXktZm9yLWluaXRpYWxpemF0aW9u");
        configuration.setBoolean("fs.azure.skipUserGroupMetadataDuringInitialization", true);
        configuration.setBoolean("fs.azure.createRemoteFileSystemDuringInitialization", false);
        try (FileSystem fileSystem = FileSystem.newInstance(URI.create(defaultFs), configuration)) {
            Assertions.assertEquals(scheme, fileSystem.getScheme());
        }
    }

    private static Map<String, Object> sourceConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(FileBaseOptions.FILE_PATH.key(), "/source");
        return config;
    }

    private static void validate(Map<String, Object> config, OptionRule optionRule) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(optionRule);
    }
}
