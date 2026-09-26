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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb.GaussDBIncrementalSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;

import lombok.Getter;

import java.io.Serializable;
import java.util.Locale;
import java.util.Optional;

/** Immutable runtime settings for the GaussDB {@code mppdb_decoding} reader. */
@Getter
public final class GaussDBMppdbConfig implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Logical decoding plugin name verified against the server-side slot. */
    private final String pluginName;

    /** Logical replication slot owned by the SeaTunnel job. */
    private final String slotName;

    /** Original JDBC URL used to derive the dedicated replication connection. */
    private final String jdbcUrl;

    /** Optional dedicated port for the GaussDB replication protocol. */
    private final Integer replicationPort;

    /** Number of server-side decoder workers. */
    private final int parallelDecodeNum;

    /** Wire representation requested from parallel mppdb decoding. */
    private final String decodeStyle;

    /** Whether the server groups decoded records into batches. */
    private final boolean sendingBatch;

    /** Creates and validates a runtime configuration from connector options. */
    public GaussDBMppdbConfig(ReadonlyConfig config) {
        this.pluginName =
                config.get(GaussDBIncrementalSourceOptions.DECODING_PLUGIN_NAME)
                        .trim()
                        .toLowerCase(Locale.ROOT);
        this.slotName = config.get(GaussDBIncrementalSourceOptions.SLOT_NAME).trim();
        this.jdbcUrl = config.get(JdbcCommonOptions.URL);
        Optional<Integer> configuredReplicationPort =
                config.getOptional(GaussDBIncrementalSourceOptions.REPLICATION_PORT);
        this.replicationPort = configuredReplicationPort.orElse(null);
        this.parallelDecodeNum = config.get(GaussDBIncrementalSourceOptions.PARALLEL_DECODE_NUM);
        this.decodeStyle =
                config.get(GaussDBIncrementalSourceOptions.DECODE_STYLE)
                        .trim()
                        .toLowerCase(Locale.ROOT);
        this.sendingBatch = config.get(GaussDBIncrementalSourceOptions.SENDING_BATCH);
        validate();
    }

    /** Returns whether this configuration selects the GaussDB-specific WAL reader. */
    public boolean usesMppdbDecoding() {
        return "mppdb_decoding".equals(pluginName);
    }

    /** Validates protocol constraints before source construction proceeds. */
    private void validate() {
        if (pluginName.isEmpty()) {
            throw new IllegalArgumentException("decoding.plugin.name must not be blank");
        }
        if (slotName.isEmpty()) {
            throw new IllegalArgumentException("slot.name must not be blank");
        }
        if (replicationPort != null && (replicationPort < 1 || replicationPort > 65535)) {
            throw new IllegalArgumentException("replication.port must be between 1 and 65535");
        }
        if (!usesMppdbDecoding()) {
            return;
        }
        if (parallelDecodeNum < 1 || parallelDecodeNum > 20) {
            throw new IllegalArgumentException("parallel-decode-num must be between 1 and 20");
        }
        if (!("b".equals(decodeStyle) || "j".equals(decodeStyle) || "t".equals(decodeStyle))) {
            throw new IllegalArgumentException("decode-style must be one of b, j, or t");
        }
    }
}
