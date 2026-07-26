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

import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;

public class PostgresSourceConfigFactoryTest {

    @Test
    public void shouldDisableDebeziumSnapshotForCommittedOffsetStartup() {
        PostgresSourceConfigFactory configFactory =
                (PostgresSourceConfigFactory)
                        new PostgresSourceConfigFactory()
                                .hostname("localhost")
                                .username("user")
                                .password("password")
                                .databaseList("database")
                                .startupOptions(
                                        new StartupConfig(
                                                StartupMode.COMMITTED_OFFSET, null, null, null));

        Assertions.assertEquals(
                "never", configFactory.create(0).getDbzConfiguration().getString("snapshot.mode"));
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
