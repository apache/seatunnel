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

package io.debezium.connector.postgresql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.annotation.ThreadSafe;
import io.debezium.connector.common.CdcSourceTaskContext;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.connector.postgresql.connection.ReplicationConnection;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;

import java.sql.SQLException;
import java.util.Collections;

/**
 * The context of a {@link PostgresConnectorTask}. This deals with most of the brunt of reading
 * various configuration options and creating other objects with these various options.
 */
@ThreadSafe
public class PostgresTaskContext extends CdcSourceTaskContext {
    protected static final Logger LOGGER = LoggerFactory.getLogger(PostgresTaskContext.class);

    private final PostgresConnectorConfig config;
    private final TopicSelector<TableId> topicSelector;
    private final PostgresSchema schema;

    private ElapsedTimeStrategy refreshXmin;
    private Long lastXmin;

    public PostgresTaskContext(
            PostgresConnectorConfig config,
            PostgresSchema schema,
            TopicSelector<TableId> topicSelector) {
        super(config.getContextName(), config.getLogicalName(), Collections::emptySet);

        this.config = config;
        if (config.xminFetchInterval().toMillis() > 0) {
            this.refreshXmin =
                    ElapsedTimeStrategy.constant(
                            Clock.SYSTEM, config.xminFetchInterval().toMillis());
        }
        this.topicSelector = topicSelector;
        assert schema != null;
        this.schema = schema;
    }

    protected TopicSelector<TableId> topicSelector() {
        return topicSelector;
    }

    protected PostgresSchema schema() {
        return schema;
    }

    protected PostgresConnectorConfig config() {
        return config;
    }

    public void refreshSchema(PostgresConnection connection, boolean printReplicaIdentityInfo)
            throws SQLException {
        schema.refresh(connection, printReplicaIdentityInfo);
    }

    Long getSlotXmin(PostgresConnection connection) throws SQLException {
        // when xmin fetch is set to 0, we don't track it to ignore any performance of querying the
        // slot periodically
        if (config.xminFetchInterval().toMillis() <= 0) {
            return null;
        }
        assert this.refreshXmin != null;

        if (this.refreshXmin.hasElapsed()) {
            lastXmin = getCurrentSlotState(connection).slotCatalogXmin();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Fetched new xmin from slot of {}", lastXmin);
            }
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace("reusing xmin value of {}", lastXmin);
            }
        }

        return lastXmin;
    }

    private SlotState getCurrentSlotState(PostgresConnection connection) throws SQLException {
        return connection.getReplicationSlotState(
                config.slotName(), config.plugin().getPostgresPluginName());
    }

    public ReplicationConnection createReplicationConnection(boolean doSnapshot)
            throws SQLException {
        final boolean dropSlotOnStop = config.dropSlotOnStop();
        if (dropSlotOnStop) {
            LOGGER.warn(
                    "Connector has enabled automated replication slot removal upon restart ({} = true). "
                            + "This setting is not recommended for production environments, as a new replication slot "
                            + "will be created after a connector restart, resulting in missed data change events.",
                    PostgresConnectorConfig.DROP_SLOT_ON_STOP.name());
        }
        return ReplicationConnection.builder(config)
                .withSlot(config.slotName())
                .withPublication(config.publicationName())
                .withTableFilter(config.getTableFilters())
                .withPublicationAutocreateMode(config.publicationAutocreateMode())
                .withPlugin(config.plugin())
                .withTruncateHandlingMode(config.truncateHandlingMode())
                .dropSlotOnClose(dropSlotOnStop)
                .streamParams(config.streamParams())
                .statusUpdateInterval(config.statusUpdateInterval())
                .withTypeRegistry(schema.getTypeRegistry())
                .doSnapshot(doSnapshot)
                .withSchema(schema)
                .build();
    }

    PostgresConnectorConfig getConfig() {
        return config;
    }
}
