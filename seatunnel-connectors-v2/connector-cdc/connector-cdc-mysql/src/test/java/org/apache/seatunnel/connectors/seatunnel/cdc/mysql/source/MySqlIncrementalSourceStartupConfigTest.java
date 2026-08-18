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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlIncrementalSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MySqlIncrementalSourceStartupConfigTest {

    private static final String GTID_SET = "3E11FA47-71CA-11E1-9E33-C80AA9429562:1-10";

    @Test
    public void testOptionRuleAcceptsGtidSpecificStartup() {
        Map<String, Object> options = requiredOptions();
        options.put(SourceOptions.STARTUP_MODE_KEY, "specific");
        options.put(SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE.key(), "mysql-bin.000123");
        options.put(SourceOptions.STARTUP_SPECIFIC_OFFSET_POS.key(), 456789L);
        options.put(MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_GTID_SET.key(), GTID_SET);

        ConfigValidator.of(ReadonlyConfig.fromMap(options))
                .validate(new MySqlIncrementalSourceFactory().optionRule());
    }

    @Test
    public void testOptionRuleAcceptsSnapshotOnlyStartup() {
        Map<String, Object> options = requiredOptions();
        options.put(SourceOptions.STARTUP_MODE_KEY, "snapshot-only");

        ConfigValidator.of(ReadonlyConfig.fromMap(options))
                .validate(new MySqlIncrementalSourceFactory().optionRule());
    }

    @Test
    public void testCreateStartupConfigAllowsSnapshotOnlyWithDefaultStopMode() {
        StartupConfig startupConfig =
                MySqlIncrementalSource.createStartupConfig(
                        config(SourceOptions.STARTUP_MODE_KEY, "snapshot-only"));

        Assertions.assertEquals(
                org.apache.seatunnel.connectors.cdc.base.option.StartupMode.SNAPSHOT_ONLY,
                startupConfig.getStartupMode());
    }

    @Test
    public void testCreateStartupConfigRejectsSnapshotOnlyWithStopMode() {
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MySqlIncrementalSource.createStartupConfig(
                                        config(
                                                SourceOptions.STARTUP_MODE_KEY,
                                                "snapshot-only",
                                                SourceOptions.STOP_MODE_KEY,
                                                "latest")));

        Assertions.assertTrue(exception.getMessage().contains("snapshot-only"));
    }

    @Test
    public void testCreateStartupConfigWithGtidSetAndSkipFields() {
        StartupConfig startupConfig =
                MySqlIncrementalSource.createStartupConfig(
                        config(
                                SourceOptions.STARTUP_MODE_KEY,
                                "specific",
                                SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE.key(),
                                "mysql-bin.000123",
                                SourceOptions.STARTUP_SPECIFIC_OFFSET_POS.key(),
                                456789L,
                                MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_GTID_SET
                                        .key(),
                                GTID_SET,
                                MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS
                                        .key(),
                                3L,
                                MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_ROWS
                                        .key(),
                                10L));

        Offset startupOffset = startupConfig.getStartupOffset(new TestOffsetFactory());

        Assertions.assertEquals(
                "mysql-bin.000123",
                startupOffset.getOffset().get(BinlogOffset.BINLOG_FILENAME_OFFSET_KEY));
        Assertions.assertEquals(
                "456789", startupOffset.getOffset().get(BinlogOffset.BINLOG_POSITION_OFFSET_KEY));
        Assertions.assertEquals(GTID_SET, startupOffset.getOffset().get(BinlogOffset.GTID_SET_KEY));
        Assertions.assertEquals(
                "3", startupOffset.getOffset().get(BinlogOffset.EVENTS_TO_SKIP_OFFSET_KEY));
        Assertions.assertEquals(
                "10", startupOffset.getOffset().get(BinlogOffset.ROWS_TO_SKIP_OFFSET_KEY));
    }

    @Test
    public void testCreateStartupConfigWithFilePositionAndSkipFields() {
        StartupConfig startupConfig =
                MySqlIncrementalSource.createStartupConfig(
                        config(
                                SourceOptions.STARTUP_MODE_KEY,
                                "specific",
                                SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE.key(),
                                "mysql-bin.000123",
                                SourceOptions.STARTUP_SPECIFIC_OFFSET_POS.key(),
                                456789L,
                                MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_ROWS
                                        .key(),
                                2L));

        Offset startupOffset = startupConfig.getStartupOffset(new TestOffsetFactory());

        Assertions.assertEquals(
                "mysql-bin.000123",
                startupOffset.getOffset().get(BinlogOffset.BINLOG_FILENAME_OFFSET_KEY));
        Assertions.assertEquals(
                "456789", startupOffset.getOffset().get(BinlogOffset.BINLOG_POSITION_OFFSET_KEY));
        Assertions.assertEquals(
                "0", startupOffset.getOffset().get(BinlogOffset.EVENTS_TO_SKIP_OFFSET_KEY));
        Assertions.assertEquals(
                "2", startupOffset.getOffset().get(BinlogOffset.ROWS_TO_SKIP_OFFSET_KEY));
    }

    @Test
    public void testSpecificStartupOffsetLoadsThroughDebeziumOffsetLoader() {
        StartupConfig startupConfig =
                MySqlIncrementalSource.createStartupConfig(
                        config(
                                SourceOptions.STARTUP_MODE_KEY,
                                "specific",
                                SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE.key(),
                                "mysql-bin.000123",
                                SourceOptions.STARTUP_SPECIFIC_OFFSET_POS.key(),
                                456789L,
                                MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_GTID_SET
                                        .key(),
                                GTID_SET,
                                MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS
                                        .key(),
                                3L,
                                MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_ROWS
                                        .key(),
                                10L));

        Offset startupOffset = startupConfig.getStartupOffset(new TestOffsetFactory());
        MySqlOffsetContext offsetContext =
                new MySqlOffsetContext.Loader(debeziumConfig()).load(startupOffset.getOffset());

        Assertions.assertEquals("mysql-bin.000123", offsetContext.getSource().binlogFilename());
        Assertions.assertEquals(456789L, offsetContext.getSource().binlogPosition());
        Assertions.assertEquals(GTID_SET, offsetContext.gtidSet());
        Assertions.assertEquals(3L, offsetContext.eventsToSkipUponRestart());
        Assertions.assertEquals(10, offsetContext.rowsToSkipUponRestart());
    }

    @Test
    public void testCreateStartupConfigRejectsGtidSetWithoutFilePosition() {
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MySqlIncrementalSource.createStartupConfig(
                                        config(
                                                SourceOptions.STARTUP_MODE_KEY,
                                                "specific",
                                                MySqlIncrementalSourceOptions
                                                        .STARTUP_SPECIFIC_OFFSET_GTID_SET
                                                        .key(),
                                                GTID_SET)));

        Assertions.assertTrue(
                exception.getMessage().contains(SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE.key()));
    }

    @Test
    public void testCreateStartupConfigRejectsMissingSpecificOffset() {
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MySqlIncrementalSource.createStartupConfig(
                                        config(SourceOptions.STARTUP_MODE_KEY, "specific")));

        Assertions.assertTrue(exception.getMessage().contains("requires"));
    }

    @Test
    public void testCreateStartupConfigRejectsSkipOutsideSpecificMode() {
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MySqlIncrementalSource.createStartupConfig(
                                        config(
                                                SourceOptions.STARTUP_MODE_KEY,
                                                "latest",
                                                MySqlIncrementalSourceOptions
                                                        .STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS
                                                        .key(),
                                                1L)));

        Assertions.assertTrue(exception.getMessage().contains("startup.specific-offset.*"));
    }

    @Test
    public void testCreateStartupConfigRejectsMalformedGtidSet() {
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MySqlIncrementalSource.createStartupConfig(
                                        config(
                                                SourceOptions.STARTUP_MODE_KEY,
                                                "specific",
                                                SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE.key(),
                                                "mysql-bin.000123",
                                                SourceOptions.STARTUP_SPECIFIC_OFFSET_POS.key(),
                                                456789L,
                                                MySqlIncrementalSourceOptions
                                                        .STARTUP_SPECIFIC_OFFSET_GTID_SET
                                                        .key(),
                                                "bad-gtid")));

        Assertions.assertTrue(exception.getMessage().contains("Invalid"));
    }

    @Test
    public void testCreateStartupConfigRejectsNegativeSkipRows() {
        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () ->
                                MySqlIncrementalSource.createStartupConfig(
                                        config(
                                                SourceOptions.STARTUP_MODE_KEY,
                                                "specific",
                                                SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE.key(),
                                                "mysql-bin.000123",
                                                SourceOptions.STARTUP_SPECIFIC_OFFSET_POS.key(),
                                                456789L,
                                                MySqlIncrementalSourceOptions
                                                        .STARTUP_SPECIFIC_OFFSET_GTID_SET
                                                        .key(),
                                                GTID_SET,
                                                MySqlIncrementalSourceOptions
                                                        .STARTUP_SPECIFIC_OFFSET_SKIP_ROWS
                                                        .key(),
                                                -1L)));

        Assertions.assertTrue(exception.getMessage().contains("greater than or equal to 0"));
    }

    private static ReadonlyConfig config(Object... keysAndValues) {
        Map<String, Object> options = new HashMap<>();
        for (int i = 0; i < keysAndValues.length; i += 2) {
            options.put((String) keysAndValues[i], keysAndValues[i + 1]);
        }
        return ReadonlyConfig.fromMap(options);
    }

    private static MySqlConnectorConfig debeziumConfig() {
        return new MySqlConnectorConfig(
                Configuration.create()
                        .with(MySqlConnectorConfig.SERVER_NAME, "test_server")
                        .with(MySqlConnectorConfig.HOSTNAME, "localhost")
                        .with(MySqlConnectorConfig.USER, "test")
                        .with(MySqlConnectorConfig.PASSWORD, "test")
                        .build());
    }

    private static Map<String, Object> requiredOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put(MySqlIncrementalSourceOptions.USERNAME.key(), "mysqluser");
        options.put(MySqlIncrementalSourceOptions.PASSWORD.key(), "mysqlpw");
        options.put(MySqlIncrementalSourceOptions.URL.key(), "jdbc:mysql://localhost:3306/test");
        options.put(
                MySqlIncrementalSourceOptions.TABLE_NAMES.key(),
                Collections.singletonList("test.table1"));
        return options;
    }

    private static class TestOffsetFactory extends OffsetFactory {
        @Override
        public Offset earliest() {
            return new TestOffset(Collections.emptyMap());
        }

        @Override
        public Offset neverStop() {
            return new TestOffset(Collections.emptyMap());
        }

        @Override
        public Offset latest() {
            return new TestOffset(Collections.emptyMap());
        }

        @Override
        public Offset specific(Map<String, String> offset) {
            return new TestOffset(offset);
        }

        @Override
        public Offset specific(String filename, Long position) {
            Map<String, String> offset = new HashMap<>();
            offset.put(BinlogOffset.BINLOG_FILENAME_OFFSET_KEY, filename);
            offset.put(BinlogOffset.BINLOG_POSITION_OFFSET_KEY, String.valueOf(position));
            return new TestOffset(offset);
        }

        @Override
        public Offset timestamp(long timestamp) {
            return new TestOffset(Collections.emptyMap());
        }
    }

    private static class TestOffset extends Offset {
        TestOffset(Map<String, String> offset) {
            this.offset = offset;
        }

        @Override
        public int compareTo(Offset offset) {
            return 0;
        }
    }
}
