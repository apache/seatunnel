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

package org.apache.seatunnel.connectors.seatunnel.cdc.pgbase.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationConverterFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.DeserializeFormat;
import org.apache.seatunnel.connectors.cdc.debezium.row.DebeziumJsonDeserializeSchema;
import org.apache.seatunnel.connectors.cdc.debezium.row.SeaTunnelRowDebeziumDeserializeSchema;

import org.apache.kafka.connect.data.Struct;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.ConnectTableChangeSerializer;
import io.debezium.relational.history.TableChanges;
import io.debezium.util.SchemaNameAdjuster;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Shared incremental source logic for PostgreSQL-compatible CDC connectors.
 *
 * <p>This phase intentionally keeps the dialect, fetch-task, and offset behavior in the concrete
 * connector modules while deduplicating the common deserialization and table-change discovery
 * logic.
 */
public abstract class PgBaseIncrementalSource<T, C extends JdbcSourceConfig>
        extends IncrementalSource<T, C> implements SupportParallelism {

    // The SeaTunnelSource is Java-serialized into the job DAG, and this class sits in that
    // hierarchy. Pinning the UID keeps a later edit here from breaking source deserialization
    // between mixed-build client/server submissions.
    private static final long serialVersionUID = 1L;

    protected PgBaseIncrementalSource(ReadonlyConfig options, List<CatalogTable> catalogTables) {
        super(options, catalogTables);
    }

    @SuppressWarnings("unchecked")
    @Override
    public DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(
            ReadonlyConfig config) {
        Map<TableId, Struct> tableIdTableChangeMap = loadTableChanges();
        if (DeserializeFormat.COMPATIBLE_DEBEZIUM_JSON.equals(
                config.get(JdbcSourceOptions.FORMAT))) {
            return (DebeziumDeserializationSchema<T>)
                    new DebeziumJsonDeserializeSchema(
                            config.get(JdbcSourceOptions.DEBEZIUM_PROPERTIES),
                            tableIdTableChangeMap);
        }

        return (DebeziumDeserializationSchema<T>)
                SeaTunnelRowDebeziumDeserializeSchema.builder()
                        .setTables(catalogTables)
                        .setServerTimeZone(
                                ZoneId.of(config.get(JdbcSourceOptions.SERVER_TIME_ZONE)))
                        .setTableIdTableChangeMap(tableIdTableChangeMap)
                        .setUserDefinedConverterFactory(getUserDefinedConverterFactory())
                        .build();
    }

    /**
     * Returns the Debezium converter factory used by the concrete PG-base connector.
     *
     * <p>Defaults to the same factory {@link SeaTunnelRowDebeziumDeserializeSchema.Builder} already
     * applies, so a dialect only overrides this when it genuinely needs custom conversion.
     */
    protected DebeziumDeserializationConverterFactory getUserDefinedConverterFactory() {
        return DebeziumDeserializationConverterFactory.DEFAULT;
    }

    /**
     * Discovers the captured tables and serializes their schemas into the Debezium table-change
     * payload used by the row deserializer.
     *
     * <p>Reuses the dialect built by the constructor instead of instantiating a second one, so the
     * discovery here shares the connector's existing connection configuration. Table discovery must
     * finish before the schema reader opens because discovery owns its JDBC connection; this keeps
     * initialization at the historical peak of one active connection.
     */
    protected Map<TableId, Struct> loadTableChanges() {
        C sourceConfig = configFactory.create(0);
        JdbcDataSourceDialect jdbcDataSourceDialect = (JdbcDataSourceDialect) dataSourceDialect;
        ConnectTableChangeSerializer serializer =
                new ConnectTableChangeSerializer(SchemaNameAdjuster.create());
        try {
            // Discovery owns its JDBC connection, so finish it before opening the schema reader.
            List<TableId> tableIds = jdbcDataSourceDialect.discoverDataCollections(sourceConfig);
            try (JdbcConnection jdbcConnection =
                    jdbcDataSourceDialect.openJdbcConnection(sourceConfig)) {
                return tableIds.stream()
                        .collect(
                                Collectors.toMap(
                                        Function.identity(),
                                        tableId ->
                                                serializeTableChange(
                                                        jdbcDataSourceDialect,
                                                        jdbcConnection,
                                                        tableId,
                                                        serializer)));
            }
        } catch (Exception e) {
            // Shared by every PG-base connector now, so name the phase: a bare wrapped
            // exception here is indistinguishable between discovery, schema query and
            // serialization failures.
            throw new SeaTunnelException(
                    "Failed to load table changes for CDC schema initialization", e);
        }
    }

    private Struct serializeTableChange(
            JdbcDataSourceDialect jdbcDataSourceDialect,
            JdbcConnection jdbcConnection,
            TableId tableId,
            ConnectTableChangeSerializer serializer) {
        TableChanges tableChanges = new TableChanges();
        tableChanges.create(
                jdbcDataSourceDialect.queryTableSchema(jdbcConnection, tableId).getTable());
        return serializer.serialize(tableChanges).get(0);
    }
}
