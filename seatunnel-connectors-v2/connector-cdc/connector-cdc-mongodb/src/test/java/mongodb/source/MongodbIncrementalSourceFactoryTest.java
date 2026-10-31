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

package mongodb.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.MongodbIncrementalSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbIncrementalSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfigProvider;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffsetFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class MongodbIncrementalSourceFactoryTest {
    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new MongodbIncrementalSourceFactory()).optionRule());
    }

    @Test
    public void testSupportedStartUpModes() {
        MongodbIncrementalSourceFactory mongodbIncrementalSourceFactory =
                new MongodbIncrementalSourceFactory();
        mongodbIncrementalSourceFactory.optionRule().getOptionalOptions().stream()
                .filter((option) -> option.key().equals(SourceOptions.STARTUP_MODE_KEY))
                .forEach(
                        (option) -> {
                            Assertions.assertIterableEquals(
                                    Arrays.asList(
                                            StartupMode.INITIAL,
                                            StartupMode.LATEST,
                                            StartupMode.TIMESTAMP),
                                    ((SingleChoiceOption<StartupMode>) option).getOptionValues());
                        });
    }

    @Test
    public void testSupportedStopModes() {
        MongodbIncrementalSourceFactory mongodbIncrementalSourceFactory =
                new MongodbIncrementalSourceFactory();
        mongodbIncrementalSourceFactory.optionRule().getOptionalOptions().stream()
                .filter((option) -> option.key().equals(SourceOptions.STOP_MODE_KEY))
                .forEach(
                        (option) ->
                                Assertions.assertIterableEquals(
                                        Arrays.asList(StopMode.NEVER, StopMode.TIMESTAMP),
                                        ((SingleChoiceOption<StopMode>) option).getOptionValues()));
    }

    @Test
    public void testSourceConfigBuilderAcceptsLatestStartupMode() {
        // Regression for the real source-assembly path: the builder used to reject
        // StartupMode.LATEST at runtime even though the option rule advertised it.
        MongodbSourceConfig config =
                MongodbSourceConfigProvider.newBuilder()
                        .hosts("localhost:27017")
                        .startupOptions(new StartupConfig(StartupMode.LATEST, null, null, null))
                        .validate()
                        .create(0);

        Assertions.assertEquals(StartupMode.LATEST, config.getStartupConfig().getStartupMode());
    }

    @Test
    public void testSourceConfigBuilderRejectsUnsupportedStartupMode() {
        Assertions.assertThrows(
                MongodbConnectorException.class,
                () ->
                        MongodbSourceConfigProvider.newBuilder()
                                .startupOptions(
                                        new StartupConfig(StartupMode.EARLIEST, null, null, null)));
    }

    @Test
    public void testLatestStartupModeResolvesToLatestChangeStreamOffset() {
        StartupConfig startupConfig = new StartupConfig(StartupMode.LATEST, null, null, null);

        Offset startupOffset = startupConfig.getStartupOffset(new ChangeStreamOffsetFactory());

        Assertions.assertInstanceOf(ChangeStreamOffset.class, startupOffset);
        // The latest offset is a current-time change-stream position, not a resume token from a
        // snapshot: starting here means only changes made after the job starts are consumed.
        Assertions.assertNotNull(((ChangeStreamOffset) startupOffset).getTimestamp());
        Assertions.assertNull(((ChangeStreamOffset) startupOffset).getResumeToken());
    }

    @Test
    public void testSourceConfigBuilderAcceptsTimestampStopMode() {
        long stopTimestamp = 1_700_000_000_000L;
        StopConfig stopConfig = new StopConfig(StopMode.TIMESTAMP, null, null, stopTimestamp);

        MongodbSourceConfig config =
                MongodbSourceConfigProvider.newBuilder()
                        .hosts("localhost:27017")
                        .stopOptions(stopConfig)
                        .validate()
                        .create(0);

        Assertions.assertEquals(StopMode.TIMESTAMP, config.getStopConfig().getStopMode());
        ChangeStreamOffset stopOffset =
                (ChangeStreamOffset)
                        config.getStopConfig().getStopOffset(new ChangeStreamOffsetFactory());
        Assertions.assertEquals(
                stopTimestamp / 1000, Integer.toUnsignedLong(stopOffset.getTimestamp().getTime()));
    }

    @Test
    public void testSourceConfigBuilderAcceptsTimestampRange() {
        MongodbSourceConfig config =
                MongodbSourceConfigProvider.newBuilder()
                        .hosts("localhost:27017")
                        .startupOptions(
                                new StartupConfig(
                                        StartupMode.TIMESTAMP, null, null, 1_700_000_000_000L))
                        .stopOptions(
                                new StopConfig(StopMode.TIMESTAMP, null, null, 1_700_000_001_000L))
                        .validate()
                        .create(0);

        Assertions.assertEquals(StartupMode.TIMESTAMP, config.getStartupConfig().getStartupMode());
        Assertions.assertEquals(StopMode.TIMESTAMP, config.getStopConfig().getStopMode());
    }

    @Test
    public void testSourceConfigBuilderRejectsInvalidTimestampRange() {
        long startupTimestamp = 1_700_000_000_000L;

        Assertions.assertThrows(
                MongodbConnectorException.class,
                () ->
                        MongodbSourceConfigProvider.newBuilder()
                                .hosts("localhost:27017")
                                .startupOptions(
                                        new StartupConfig(
                                                StartupMode.TIMESTAMP,
                                                null,
                                                null,
                                                startupTimestamp))
                                .stopOptions(
                                        new StopConfig(
                                                StopMode.TIMESTAMP, null, null, startupTimestamp))
                                .validate());
        Assertions.assertThrows(
                MongodbConnectorException.class,
                () ->
                        MongodbSourceConfigProvider.newBuilder()
                                .hosts("localhost:27017")
                                .startupOptions(
                                        new StartupConfig(
                                                StartupMode.TIMESTAMP,
                                                null,
                                                null,
                                                startupTimestamp))
                                .stopOptions(
                                        new StopConfig(
                                                StopMode.TIMESTAMP,
                                                null,
                                                null,
                                                startupTimestamp - 1000L))
                                .validate());
        MongodbConnectorException sameSecondException =
                Assertions.assertThrows(
                        MongodbConnectorException.class,
                        () ->
                                MongodbSourceConfigProvider.newBuilder()
                                        .hosts("localhost:27017")
                                        .startupOptions(
                                                new StartupConfig(
                                                        StartupMode.TIMESTAMP,
                                                        null,
                                                        null,
                                                        startupTimestamp))
                                        .stopOptions(
                                                new StopConfig(
                                                        StopMode.TIMESTAMP,
                                                        null,
                                                        null,
                                                        startupTimestamp + 500L))
                                        .validate());
        Assertions.assertTrue(sameSecondException.getMessage().contains("second precision"));
    }

    @Test
    public void testSourceConfigBuilderRejectsUnsupportedStopMode() {
        Assertions.assertThrows(
                MongodbConnectorException.class,
                () ->
                        MongodbSourceConfigProvider.newBuilder()
                                .stopOptions(new StopConfig(StopMode.SPECIFIC, null, null, null)));
        Assertions.assertThrows(
                MongodbConnectorException.class,
                () ->
                        MongodbSourceConfigProvider.newBuilder()
                                .stopOptions(new StopConfig(StopMode.LATEST, null, null, null)));
    }

    @Test
    public void testOptionRuleAcceptsValidValueDomains() {
        assertValid(basicOptions());
    }

    @Test
    public void testOptionRuleAcceptsBoundaryNumericOptions() {
        Map<String, Object> options = basicOptions();
        options.put(MongodbIncrementalSourceOptions.BATCH_SIZE.key(), 0);
        options.put(MongodbIncrementalSourceOptions.POLL_MAX_BATCH_SIZE.key(), 1);
        options.put(MongodbIncrementalSourceOptions.POLL_AWAIT_TIME_MILLIS.key(), 1);
        options.put(MongodbIncrementalSourceOptions.HEARTBEAT_INTERVAL_MILLIS.key(), 0);
        options.put(MongodbIncrementalSourceOptions.INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB.key(), 1);
        assertValid(options);
    }

    @Test
    public void testOptionRuleRejectsOutOfRangeNumericOptions() {
        Object[][] invalidValues = {
            {MongodbIncrementalSourceOptions.BATCH_SIZE, -1, ">= 0"},
            {MongodbIncrementalSourceOptions.POLL_MAX_BATCH_SIZE, 0, "> 0"},
            {MongodbIncrementalSourceOptions.POLL_AWAIT_TIME_MILLIS, 0, "> 0"},
            {MongodbIncrementalSourceOptions.HEARTBEAT_INTERVAL_MILLIS, -1, ">= 0"},
            {MongodbIncrementalSourceOptions.INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB, 0, "> 0"},
        };
        for (Object[] invalidValue : invalidValues) {
            Option<?> option = (Option<?>) invalidValue[0];
            String operator = (String) invalidValue[2];
            Map<String, Object> options = basicOptions();
            options.put(option.key(), invalidValue[1]);
            OptionValidationException error = assertValidationFails(options);
            Assertions.assertTrue(
                    error.getMessage().contains(option.key())
                            && error.getMessage().contains(operator),
                    () ->
                            option.key()
                                    + " must be rejected by '"
                                    + operator
                                    + "': "
                                    + error.getMessage());
        }
    }

    @Test
    public void testOptionRuleAcceptsTableConfigsInsteadOfSchema() {
        Map<String, Object> options = basicOptions();
        options.remove(MongodbIncrementalSourceOptions.SCHEMA.key());
        options.put(
                MongodbIncrementalSourceOptions.TABLE_CONFIGS.key(),
                Collections.singletonList(schemaEntry()));
        assertValid(options);
    }

    @Test
    public void testOptionRuleRejectsEmptySchema() {
        Map<String, Object> options = basicOptions();
        options.put(MongodbIncrementalSourceOptions.SCHEMA.key(), new HashMap<String, Object>());
        OptionValidationException error = assertValidationFails(options);
        Assertions.assertTrue(error.getMessage().contains("is not empty"), error.getMessage());
    }

    @Test
    public void testOptionRuleRejectsEmptyTableConfigs() {
        Map<String, Object> options = basicOptions();
        options.remove(MongodbIncrementalSourceOptions.SCHEMA.key());
        options.put(
                MongodbIncrementalSourceOptions.TABLE_CONFIGS.key(),
                new ArrayList<Map<String, Object>>());
        OptionValidationException error = assertValidationFails(options);
        Assertions.assertTrue(error.getMessage().contains("is not empty"), error.getMessage());
    }

    @Test
    public void testOptionRuleKeepsSchemaAndTableConfigsExclusive() {
        Map<String, Object> options = basicOptions();
        options.put(
                MongodbIncrementalSourceOptions.TABLE_CONFIGS.key(),
                Collections.singletonList(schemaEntry()));
        OptionValidationException error = assertValidationFails(options);
        Assertions.assertTrue(
                error.getMessage().contains("mutually exclusive"), error.getMessage());
    }

    @Test
    public void testOptionRuleRejectsMissingSchemaAndTableConfigs() {
        Map<String, Object> options = basicOptions();
        options.remove(MongodbIncrementalSourceOptions.SCHEMA.key());
        OptionValidationException error = assertValidationFails(options);
        Assertions.assertTrue(
                error.getMessage().contains("exactly one option must be set"), error.getMessage());
    }

    @Test
    public void testOptionRuleRequiresStopTimestampWhenStopModeIsTimestamp() {
        // stop.timestamp is required when stop.mode is timestamp; the range check added along with
        // it must not shadow that requirement.
        Map<String, Object> options = basicOptions();
        options.put(MongodbIncrementalSourceOptions.STOP_MODE.key(), "timestamp");
        OptionValidationException error = assertValidationFails(options);
        Assertions.assertTrue(
                error.getMessage().contains(SourceOptions.STOP_TIMESTAMP.key()),
                error.getMessage());
    }

    @Test
    public void testOptionRuleAcceptsTimestampRange() {
        assertValid(timestampRangeOptions(1_700_000_000_000L, 1_700_000_001_000L));
    }

    @Test
    public void testOptionRuleRejectsTimestampRangeThatDoesNotAdvance() {
        long startupTimestamp = 1_700_000_000_000L;

        // Comparing raw milliseconds would accept both of these; the positions they resolve to are
        // what matters, and a change-stream timestamp only carries second precision.
        OptionValidationException sameSecond =
                assertValidationFails(
                        timestampRangeOptions(startupTimestamp, startupTimestamp + 500L));
        Assertions.assertTrue(
                sameSecond.getMessage().contains("second precision"), sameSecond.getMessage());
        assertValidationFails(timestampRangeOptions(startupTimestamp, startupTimestamp));
        assertValidationFails(timestampRangeOptions(startupTimestamp, startupTimestamp - 1000L));
    }

    @Test
    public void testOptionRuleIgnoresTimestampRangeWhenStartupModeIsNotTimestamp() {
        Map<String, Object> options = timestampRangeOptions(1_700_000_000_000L, 1_700_000_000_000L);
        options.put(MongodbIncrementalSourceOptions.STARTUP_MODE.key(), "latest");
        options.remove(SourceOptions.STARTUP_TIMESTAMP.key());
        assertValid(options);
    }

    private static OptionRule optionRule() {
        return new MongodbIncrementalSourceFactory().optionRule();
    }

    private static Map<String, Object> basicOptions() {
        Map<String, Object> options = new HashMap<>();
        options.put(MongodbIncrementalSourceOptions.HOSTS.key(), "localhost:27017");
        options.put(
                MongodbIncrementalSourceOptions.DATABASE.key(),
                Collections.singletonList("inventory"));
        options.put(
                MongodbIncrementalSourceOptions.COLLECTION.key(),
                Collections.singletonList("products"));
        options.put(MongodbIncrementalSourceOptions.SCHEMA.key(), schemaEntry());
        return options;
    }

    private static Map<String, Object> schemaEntry() {
        Map<String, Object> table = new HashMap<>();
        table.put("fields", Collections.singletonMap("id", "string"));
        return Collections.singletonMap("inventory.products", table);
    }

    private static Map<String, Object> timestampRangeOptions(
            long startupTimestamp, long stopTimestamp) {
        Map<String, Object> options = basicOptions();
        options.put(MongodbIncrementalSourceOptions.STARTUP_MODE.key(), "timestamp");
        options.put(SourceOptions.STARTUP_TIMESTAMP.key(), startupTimestamp);
        options.put(MongodbIncrementalSourceOptions.STOP_MODE.key(), "timestamp");
        options.put(SourceOptions.STOP_TIMESTAMP.key(), stopTimestamp);
        return options;
    }

    private static void assertValid(Map<String, Object> options) {
        ConfigValidator.of(ReadonlyConfig.fromMap(options)).validate(optionRule());
    }

    private static OptionValidationException assertValidationFails(Map<String, Object> options) {
        return Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(options)).validate(optionRule()));
    }
}
