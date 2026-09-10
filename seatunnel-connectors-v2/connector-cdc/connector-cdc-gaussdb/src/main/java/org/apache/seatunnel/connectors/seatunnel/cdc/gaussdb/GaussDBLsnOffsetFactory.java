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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.offset.LsnOffsetFactory;

import io.debezium.connector.postgresql.PostgresOffsetContext;
import io.debezium.connector.postgresql.SourceInfo;

import java.util.HashMap;
import java.util.Map;

/** Offset factory that prepares the mppdb slot before latest-offset discovery. */
final class GaussDBLsnOffsetFactory extends OffsetFactory {

    /** Existing PostgreSQL LSN implementation reused for offset serialization and slot queries. */
    private final LsnOffsetFactory delegate;

    /** Source configuration used when slot preparation requires a new JDBC connection. */
    private final PostgresSourceConfig sourceConfig;

    /** GaussDB dialect owning mppdb slot preparation. */
    private final GaussDBDialect dialect;

    /** Creates a checkpoint-compatible offset factory. */
    GaussDBLsnOffsetFactory(PostgresSourceConfigFactory configFactory, GaussDBDialect dialect) {
        this.delegate = new LsnOffsetFactory(configFactory, dialect);
        this.sourceConfig = configFactory.create(0);
        this.dialect = dialect;
    }

    /** Returns the logical beginning marker used by SeaTunnel CDC. */
    @Override
    public Offset earliest() {
        return delegate.earliest();
    }

    /** Returns the unbounded stopping marker. */
    @Override
    public Offset neverStop() {
        return delegate.neverStop();
    }

    /**
     * Creates the slot first so changes cannot fall between offset discovery and stream startup.
     */
    @Override
    public Offset latest() {
        dialect.ensureSlot(sourceConfig);
        return delegate.latest();
    }

    /** Reads the confirmed flush LSN from an explicitly managed replication slot. */
    @Override
    public Offset committedOffset() {
        return delegate.committedOffset();
    }

    /** Restores an LSN from serialized checkpoint state. */
    @Override
    public Offset specific(Map<String, String> offset) {
        return delegate.specific(toCheckpointOffset(offset));
    }

    /** Returns a copy whose primary LSN is the last complete transaction boundary. */
    static Map<String, String> toCheckpointOffset(Map<String, String> offset) {
        Map<String, String> checkpointOffset = new HashMap<>(offset);
        String completelyProcessedLsn =
                checkpointOffset.get(PostgresOffsetContext.LAST_COMPLETELY_PROCESSED_LSN_KEY);
        if (completelyProcessedLsn != null) {
            // Debezium metadata retains the row LSN, while SeaTunnel state must advance only to a
            // transaction boundary that is safe for slot acknowledgement and recovery.
            checkpointOffset.put(SourceInfo.LSN_KEY, completelyProcessedLsn);
        }
        return checkpointOffset;
    }

    /** PostgreSQL LSNs do not use filename/position pairs. */
    @Override
    public Offset specific(String filename, Long position) {
        return delegate.specific(filename, position);
    }

    /** Timestamp startup is not supported by the PostgreSQL-compatible LSN implementation. */
    @Override
    public Offset timestamp(long timestamp) {
        return delegate.timestamp(timestamp);
    }
}
