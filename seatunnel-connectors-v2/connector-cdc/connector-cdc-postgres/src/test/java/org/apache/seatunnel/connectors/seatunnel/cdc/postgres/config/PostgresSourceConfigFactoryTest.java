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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.PostgresIncrementalSourceFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.PostgresSourceOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

/** Tests the PG-base-backed PostgreSQL source config factory behavior that must stay compatible. */
public class PostgresSourceConfigFactoryTest {

    @Test
    public void shouldDeclareStopModeInRuntimeFactoryRule() {
        Option<?> stopMode =
                new PostgresIncrementalSourceFactory()
                        .optionRule().getOptionalOptions().stream()
                                .filter(option -> "stop.mode".equals(option.key()))
                                .findFirst()
                                .orElseThrow(
                                        () -> new AssertionError("Factory must declare stop.mode"));
        Assertions.assertEquals(PostgresSourceOptions.STOP_MODE, stopMode);
        Assertions.assertTrue(stopMode instanceof SingleChoiceOption);
        Assertions.assertEquals(
                Collections.singletonList(StopMode.NEVER),
                ((SingleChoiceOption<?>) stopMode).getOptionValues());
    }

    @Test
    public void shouldKeepNeverAsDefaultStopMode() {
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.emptyMap());

        Assertions.assertEquals(StopMode.NEVER, config.get(PostgresSourceOptions.STOP_MODE));
        Assertions.assertDoesNotThrow(() -> ConfigValidator.of(config).validate(stopModeRule()));
    }

    @Test
    public void shouldAcceptExplicitNeverStopMode() {
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(Collections.singletonMap("stop.mode", "never"));

        Assertions.assertDoesNotThrow(() -> ConfigValidator.of(config).validate(stopModeRule()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"specific", "latest", "timestamp"})
    public void shouldRejectUnsupportedBoundedStopMode(String mode) {
        ReadonlyConfig config = ReadonlyConfig.fromMap(Collections.singletonMap("stop.mode", mode));

        OptionValidationException error =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(config).validate(stopModeRule()));
        Assertions.assertTrue(error.getMessage().contains("stop.mode"));
        Assertions.assertTrue(error.getMessage().contains(mode.toUpperCase(Locale.ROOT)));
    }

    private static OptionRule stopModeRule() {
        return OptionRule.builder().optional(PostgresSourceOptions.STOP_MODE).build();
    }

    @Test
    public void testCreateFormatsSchemaQualifiedTableIdentifiers() {
        PostgresSourceConfigFactory factory = baseFactory();
        factory.tableList("inventory.orders", "db1.public.customers");

        PostgresSourceConfig sourceConfig = factory.create(0);

        Assertions.assertEquals(
                "inventory.orders,public.customers",
                sourceConfig.getDbzConfiguration().getString("table.include.list"));
    }

    @Test
    public void testCreateRejectsInvalidTableIdentifier() {
        PostgresSourceConfigFactory factory = baseFactory();
        factory.tableList("orders");

        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> factory.create(0));

        // Pin the wording: this message is user-facing and predates the PG-base extraction.
        Assertions.assertEquals(
                "Invalid table name: orders ,Postgres identifier is of the form schemaName.tableName",
                exception.getMessage());
    }

    @Test
    public void shouldDisableDebeziumSnapshotForCommittedOffsetStartup() {
        PostgresSourceConfigFactory factory = baseFactory();
        factory.startupOptions(new StartupConfig(StartupMode.COMMITTED_OFFSET, null, null, null));

        Assertions.assertEquals(
                "never", factory.create(0).getDbzConfiguration().getString("snapshot.mode"));
        // "database.include.list" must stay unset: Debezium turns it into a catalog predicate on
        // dataCollectionFilter(), which rejects the catalog-less TableIds used by PostgreSQL.
        Assertions.assertNull(
                factory.create(0).getDbzConfiguration().getString("database.include.list"));
    }

    @Test
    public void shouldRunDebeziumSnapshotOnlyForSnapshotOnlyStartup() {
        PostgresSourceConfigFactory factory = baseFactory();
        factory.startupOptions(new StartupConfig(StartupMode.SNAPSHOT_ONLY, null, null, null));

        Assertions.assertEquals(
                "initial_only", factory.create(0).getDbzConfiguration().getString("snapshot.mode"));
    }

    @Test
    public void shouldLeaveSnapshotModeUnsetForInitialStartup() {
        PostgresSourceConfigFactory factory = baseFactory();

        Assertions.assertNull(
                factory.create(0).getDbzConfiguration().getString("snapshot.mode"),
                "initial startup must keep the Debezium default snapshot mode");
    }

    @Test
    public void shouldKeepPostgresSpecificDebeziumProperties() {
        PostgresSourceConfigFactory factory = baseFactory();
        factory.tableList("inventory.orders");

        PostgresSourceConfig sourceConfig = factory.create(0);

        Assertions.assertEquals(
                "postgres_cdc_source",
                sourceConfig.getDbzConfiguration().getString("database.server.name"));
        Assertions.assertEquals(
                PostgresIncrementalSourceOptions.DECODING_PLUGIN_NAME.defaultValue(),
                sourceConfig.getDbzConfiguration().getString("plugin.name"));
        Assertions.assertEquals(
                PostgresIncrementalSourceOptions.SLOT_NAME.defaultValue(),
                sourceConfig.getDbzConfiguration().getString("slot.name"));
        Assertions.assertEquals("org.postgresql.Driver", sourceConfig.getDriverClassName());
    }

    @Test
    public void shouldLetUserDebeziumPropertiesOverrideDefaults() {
        PostgresSourceConfigFactory factory = baseFactory();
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty("slot.name", "custom_slot");
        factory.debeziumProperties(dbzProperties);

        Assertions.assertEquals(
                "custom_slot", factory.create(0).getDbzConfiguration().getString("slot.name"));
    }

    @Test
    public void shouldRejectInvalidConfiguredSlotName() {
        Map<String, Object> options = new HashMap<>();
        options.put("hostname", "127.0.0.1");
        options.put("username", "user");
        options.put("password", "pwd");
        options.put("database-names", Collections.singletonList("inventory"));
        options.put("slot.name", "invalid-slot");
        PostgresSourceConfigFactory factory =
                new PostgresSourceConfigFactory()
                        .fromReadonlyConfig(ReadonlyConfig.fromMap(options));

        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> factory.create(0));

        Assertions.assertTrue(exception.getMessage().contains("slot.name 'invalid-slot'"));
    }

    @Test
    public void shouldRejectInvalidSlotNameFromDebeziumProperties() {
        PostgresSourceConfigFactory factory = baseFactory();
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty("slot.name", "invalid-slot");
        factory.debeziumProperties(dbzProperties);

        IllegalArgumentException exception =
                Assertions.assertThrows(IllegalArgumentException.class, () -> factory.create(0));

        Assertions.assertTrue(exception.getMessage().contains("slot.name 'invalid-slot'"));
    }

    /**
     * Pins the RELATION-message schema-evolution wiring: {@code include.schema.changes} must
     * reflect the actual {@code schema-changes.enabled} option instead of being hardcoded, since
     * Debezium PostgreSQL only emits the RELATION messages {@link PostgresIncrementalSource}'s
     * schema-change resolver depends on when this flag is set.
     */
    @Test
    public void shouldEnableSchemaChangesWhenSchemaEvolutionIsEnabled() {
        PostgresSourceConfigFactory factory = baseFactory();
        factory.schemaChangeEnabled(true);

        Assertions.assertEquals(
                "true",
                factory.create(0).getDbzConfiguration().getString("include.schema.changes"));
    }

    @Test
    public void shouldDisableSchemaChangesByDefault() {
        PostgresSourceConfigFactory factory = baseFactory();

        Assertions.assertEquals(
                "false",
                factory.create(0).getDbzConfiguration().getString("include.schema.changes"));
    }

    /**
     * SeaTunnel's own {@code schema-changes.enabled} option must stay authoritative even if a raw
     * {@code debezium.*} passthrough property also happens to set {@code include.schema.changes}.
     */
    @Test
    public void shouldKeepSchemaChangesEnabledAuthoritativeOverUserDebeziumProperties() {
        PostgresSourceConfigFactory factory = baseFactory();
        factory.schemaChangeEnabled(true);
        Properties dbzProperties = new Properties();
        dbzProperties.setProperty("include.schema.changes", "false");
        factory.debeziumProperties(dbzProperties);

        Assertions.assertEquals(
                "true",
                factory.create(0).getDbzConfiguration().getString("include.schema.changes"));
    }

    private PostgresSourceConfigFactory baseFactory() {
        PostgresSourceConfigFactory factory = new PostgresSourceConfigFactory();
        factory.hostname("127.0.0.1");
        factory.port(5432);
        factory.username("user");
        factory.password("pwd");
        factory.originUrl("jdbc:postgresql://127.0.0.1:5432/test");
        factory.databaseList("inventory");
        factory.startupOptions(new StartupConfig(StartupMode.INITIAL, null, null, null));
        factory.stopOptions(new StopConfig(StopMode.NEVER, null, null, null));
        return factory;
    }

    /** Verifies that parallel readers cannot consume the same temporary replication slot. */
    @Test
    public void shouldCreateIsolatedBackfillSlotName() {
        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname("localhost")
                                .username("user")
                                .password("password")
                                .databaseList("database");

        PostgresSourceConfig sourceConfig = configFactory.create(7);
        String configuredSlotName = sourceConfig.getDbzConfiguration().getString("slot.name");
        String backfillSlotName = sourceConfig.getSlotNameForBackfillTask();
        Assertions.assertNotEquals(configuredSlotName, backfillSlotName);
        Assertions.assertTrue(backfillSlotName.contains("_st_backfill_"));
        Assertions.assertTrue(backfillSlotName.endsWith("_7"));
        Assertions.assertEquals(backfillSlotName, sourceConfig.getSlotNameForBackfillTask());
    }

    /** Verifies that the reader suffix survives PostgreSQL identifier truncation. */
    @Test
    public void shouldKeepBackfillSlotNameWithinPostgresIdentifierLimit() {
        String configuredSlotName =
                "seatunnel_snapshot_backfill_slot_name_that_is_longer_than_postgres_limit";

        String backfillSlotName =
                PostgresSourceConfig.createBackfillSlotName(configuredSlotName, 12);

        Assertions.assertEquals(63, backfillSlotName.length());
        Assertions.assertTrue(backfillSlotName.contains("_st_backfill_"));
        Assertions.assertTrue(backfillSlotName.endsWith("_12"));
    }

    /** Verifies that a maximum-length configured slot cannot collide with its backfill slot. */
    @Test
    public void shouldAvoidCollisionWithConfiguredSlotName() {
        String configuredSlotName =
                "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa_st_backfill_0";

        String backfillSlotName =
                PostgresSourceConfig.createBackfillSlotName(configuredSlotName, 0);

        Assertions.assertNotEquals(configuredSlotName, backfillSlotName);
        Assertions.assertEquals(63, backfillSlotName.length());
        Assertions.assertTrue(backfillSlotName.endsWith("_0"));
    }

    /**
     * Verifies that truncation cannot collapse distinct configured slots onto one backfill slot.
     */
    @Test
    public void shouldKeepTruncatedConfiguredSlotsIsolated() {
        String commonPrefix = String.join("", Collections.nCopies(62, "a"));

        String firstBackfillSlot =
                PostgresSourceConfig.createBackfillSlotName(commonPrefix + "x", 0);
        String secondBackfillSlot =
                PostgresSourceConfig.createBackfillSlotName(commonPrefix + "y", 0);

        Assertions.assertNotEquals(firstBackfillSlot, secondBackfillSlot);
        Assertions.assertEquals(63, firstBackfillSlot.length());
        Assertions.assertEquals(63, secondBackfillSlot.length());
    }
}
