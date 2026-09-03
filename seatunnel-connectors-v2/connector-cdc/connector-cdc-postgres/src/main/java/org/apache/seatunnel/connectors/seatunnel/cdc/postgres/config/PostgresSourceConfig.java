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

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StopConfig;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.relational.RelationalTableFilters;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class PostgresSourceConfig extends JdbcSourceConfig {
    private static final long serialVersionUID = 1L;

    /**
     * PostgreSQL truncates identifiers that exceed this byte limit.
     *
     * <p>Generated temporary slot identifiers must stay within the same limit.
     */
    private static final int MAX_REPLICATION_SLOT_NAME_LENGTH = 63;

    /**
     * Lowercase hexadecimal digits used to encode the stable slot-name digest.
     *
     * <p>Replication slot names accept these characters without quoting.
     */
    private static final char[] HEX_DIGITS = "0123456789abcdef".toCharArray();

    /**
     * Identifies the reader that owns the temporary snapshot backfill slot.
     *
     * <p>Each parallel reader receives a distinct slot identifier.
     */
    private int subtaskId;

    public PostgresSourceConfig(
            StartupConfig startupConfig,
            StopConfig stopConfig,
            List<String> databaseList,
            List<String> tableList,
            int splitSize,
            Map<String, String> splitColumn,
            double distributionFactorUpper,
            double distributionFactorLower,
            int sampleShardingThreshold,
            int inverseSamplingRate,
            boolean sampleShardingAllow,
            Properties dbzProperties,
            String driverClassName,
            String hostname,
            int port,
            String username,
            String password,
            String originUrl,
            int fetchSize,
            String serverTimeZone,
            long connectTimeoutMillis,
            int connectMaxRetries,
            int connectionPoolSize,
            boolean exactlyOnce) {
        super(
                startupConfig,
                stopConfig,
                databaseList,
                tableList,
                splitSize,
                splitColumn,
                distributionFactorUpper,
                distributionFactorLower,
                sampleShardingThreshold,
                inverseSamplingRate,
                sampleShardingAllow,
                dbzProperties,
                driverClassName,
                hostname,
                port,
                username,
                password,
                originUrl,
                fetchSize,
                serverTimeZone,
                connectTimeoutMillis,
                connectMaxRetries,
                connectionPoolSize,
                exactlyOnce);
    }

    @Override
    public PostgresConnectorConfig getDbzConnectorConfig() {
        return new PostgresConnectorConfig(getDbzConfiguration());
    }

    public RelationalTableFilters getTableFilters() {
        return getDbzConnectorConfig().getTableFilters();
    }

    /**
     * Returns an isolated temporary slot name for snapshot WAL backfill.
     *
     * <p>PostgreSQL allows only one active consumer per logical replication slot, so snapshot
     * readers must not share the configured streaming slot. PostgreSQL identifiers are limited to
     * 63 bytes; slot names are ASCII identifiers, so truncating by character count is safe here.
     */
    public String getSlotNameForBackfillTask() {
        return createBackfillSlotName(
                getDbzConfiguration().getString(PostgresConnectorConfig.SLOT_NAME), subtaskId);
    }

    /**
     * Assigns the reader id after the source config is created.
     *
     * <p>The id becomes part of the temporary snapshot backfill slot name.
     */
    void setSubtaskId(int subtaskId) {
        this.subtaskId = subtaskId;
    }

    /**
     * Appends a stable source-slot digest and reader id while preserving PostgreSQL's identifier
     * length limit.
     */
    static String createBackfillSlotName(String slotName, int subtaskId) {
        String suffix = "_st_backfill_" + stableSlotHash(slotName) + "_" + subtaskId;
        String backfillSlotName = appendSuffixWithinIdentifierLimit(slotName, suffix);
        if (backfillSlotName.equals(slotName)) {
            // Preserve the suffix while making the temporary identifier different even for the
            // theoretical fixed point where a configured slot already equals its derived name.
            char firstCharacter = backfillSlotName.charAt(0) == 'z' ? 'y' : 'z';
            backfillSlotName = firstCharacter + backfillSlotName.substring(1);
        }
        return backfillSlotName;
    }

    /**
     * Returns a compact deterministic digest so different truncated source slots remain isolated.
     */
    private static String stableSlotHash(String slotName) {
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(slotName.getBytes(StandardCharsets.UTF_8));
            char[] encoded = new char[16];
            for (int i = 0; i < 8; i++) {
                int value = digest[i] & 0xff;
                encoded[i * 2] = HEX_DIGITS[value >>> 4];
                encoded[i * 2 + 1] = HEX_DIGITS[value & 0x0f];
            }
            return new String(encoded);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    /**
     * Replaces the tail of a maximum-length configured slot without truncating the reader suffix.
     */
    private static String appendSuffixWithinIdentifierLimit(String slotName, String suffix) {
        int maxBaseLength = MAX_REPLICATION_SLOT_NAME_LENGTH - suffix.length();
        String truncatedSlotName =
                slotName.length() > maxBaseLength ? slotName.substring(0, maxBaseLength) : slotName;
        return truncatedSlotName + suffix;
    }
}
