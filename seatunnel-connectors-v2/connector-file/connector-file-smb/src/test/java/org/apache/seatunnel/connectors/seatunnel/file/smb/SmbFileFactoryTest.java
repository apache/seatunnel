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

package org.apache.seatunnel.connectors.seatunnel.file.smb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.FileBaseSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.config.HadoopConf;
import org.apache.seatunnel.connectors.seatunnel.file.smb.config.SmbConf;
import org.apache.seatunnel.connectors.seatunnel.file.smb.config.SmbFileSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.file.smb.sink.SmbFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.file.smb.source.SmbFileSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.file.smb.system.SmbFileSystem;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class SmbFileFactoryTest {

    @Test
    void sourceOptionRule() {
        OptionRule optionRule = (new SmbFileSourceFactory()).optionRule();
        Assertions.assertNotNull(optionRule);
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.SYNC_MODE));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.TARGET_HADOOP_CONF));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.UPDATE_STRATEGY));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(FileBaseSourceOptions.COMPARE_MODE));
        Assertions.assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(FileBaseSourceOptions.UPDATE_COMPARE_PARALLELISM));
        Assertions.assertTrue(
                optionRule
                        .getOptionalOptions()
                        .contains(FileBaseSourceOptions.UPDATE_COMPARE_BULK_THRESHOLD));
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
    }

    @Test
    void connectionParamsAreOptional() {
        OptionRule optionRule = (new SmbFileSourceFactory()).optionRule();
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(SmbFileSourceOptions.SMB_HOST));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(SmbFileSourceOptions.SMB_USER));
        Assertions.assertTrue(
                optionRule.getOptionalOptions().contains(SmbFileSourceOptions.SMB_SHARE));
    }

    @Test
    void tablesConfigsModeValidation() {
        OptionRule optionRule = (new SmbFileSourceFactory()).optionRule();
        Map<String, Object> tableEntry = new HashMap<>();
        tableEntry.put("host", "192.168.1.100");
        tableEntry.put("user", "seatunnel");
        tableEntry.put("share", "data");
        tableEntry.put(FileBaseOptions.FILE_PATH.key(), "/data");
        tableEntry.put("file_format_type", "json");
        List<Map<String, Object>> tableConfigs = new ArrayList<>();
        tableConfigs.add(tableEntry);
        Map<String, Object> config = new HashMap<>();
        config.put("tables_configs", tableConfigs);
        Assertions.assertDoesNotThrow(() -> validate(config, optionRule));
    }

    @Test
    void sinkOptionRule() {
        OptionRule sinkOptionRule = (new SmbFileSinkFactory()).optionRule();
        Assertions.assertNotNull(sinkOptionRule);
    }

    @Test
    void sourceFactoryIdentifier() {
        Assertions.assertEquals("SmbFile", new SmbFileSourceFactory().factoryIdentifier());
    }

    @Test
    void sinkFactoryIdentifier() {
        Assertions.assertEquals("SmbFile", new SmbFileSinkFactory().factoryIdentifier());
    }

    @Test
    void syncUpdateRequiresTargetPath() {
        OptionRule optionRule = (new SmbFileSourceFactory()).optionRule();
        Map<String, Object> config = sourceConfig();
        config.put(FileBaseSourceOptions.SYNC_MODE.key(), "update");

        Assertions.assertThrows(
                OptionValidationException.class, () -> validate(config, optionRule));

        config.put(FileBaseSourceOptions.TARGET_PATH.key(), "/target");
        Assertions.assertDoesNotThrow(() -> validate(config, optionRule));
    }

    @Test
    void postSyncActionValidation() {
        OptionRule optionRule = (new SmbFileSourceFactory()).optionRule();
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
    void buildWithConfigShouldFailWithoutHost() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("user", "admin");
        configMap.put("share", "data");
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> SmbConf.buildWithConfig(ReadonlyConfig.fromMap(configMap)));
    }

    @Test
    void buildWithConfigShouldFailWithoutUser() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "myhost");
        configMap.put("share", "data");
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> SmbConf.buildWithConfig(ReadonlyConfig.fromMap(configMap)));
    }

    @Test
    void buildWithConfigShouldFailWithoutShare() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "myhost");
        configMap.put("user", "admin");
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> SmbConf.buildWithConfig(ReadonlyConfig.fromMap(configMap)));
    }

    @Test
    void buildHadoopConf() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "smb.example.com");
        configMap.put("port", 445);
        configMap.put("user", "seatunnel");
        configMap.put("password", "secret");
        configMap.put("domain", "WORKGROUP");
        configMap.put("share", "data");

        HadoopConf hadoopConf = SmbConf.buildWithConfig(ReadonlyConfig.fromMap(configMap));

        Assertions.assertEquals(
                "smb.example.com", hadoopConf.getExtraOptions().get(SmbFileSystem.FS_SMB_HOST));
        Assertions.assertEquals("445", hadoopConf.getExtraOptions().get(SmbFileSystem.FS_SMB_PORT));
        Assertions.assertEquals(
                "seatunnel", hadoopConf.getExtraOptions().get(SmbFileSystem.FS_SMB_USER));
        Assertions.assertEquals(
                "secret", hadoopConf.getExtraOptions().get(SmbFileSystem.FS_SMB_PASSWORD));
        Assertions.assertEquals(
                "WORKGROUP", hadoopConf.getExtraOptions().get(SmbFileSystem.FS_SMB_DOMAIN));
        Assertions.assertEquals(
                "data", hadoopConf.getExtraOptions().get(SmbFileSystem.FS_SMB_SHARE));
    }

    @Test
    void buildHadoopConfDefaultFS() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("host", "192.168.1.100");
        configMap.put("port", 445);
        configMap.put("user", "admin");
        configMap.put("share", "shared");

        HadoopConf hadoopConf = SmbConf.buildWithConfig(ReadonlyConfig.fromMap(configMap));

        Assertions.assertEquals("smb://192.168.1.100:445", hadoopConf.getHdfsNameKey());
        Assertions.assertEquals("smb", hadoopConf.getSchema());
    }

    private static Map<String, Object> sourceConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(FileBaseOptions.FILE_PATH.key(), "/source");
        config.put("host", "smb.example.com");
        config.put("user", "seatunnel");
        config.put("share", "data");
        return config;
    }

    private static void validate(Map<String, Object> config, OptionRule optionRule) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(optionRule);
    }
}
