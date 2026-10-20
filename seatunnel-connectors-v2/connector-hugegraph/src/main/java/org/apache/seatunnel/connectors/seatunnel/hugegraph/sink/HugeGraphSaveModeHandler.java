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

package org.apache.seatunnel.connectors.seatunnel.hugegraph.sink;

import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphDataSaveMode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSchemaSaveMode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.MappingConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.exception.HugeGraphConnectorException;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.SchemaManager;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.SchemaValidator;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Handles HugeGraph schema and data save modes on the coordinator, once per job, via the engine's
 * {@link SaveModeHandler} contract. Doing this here (instead of in the {@code HugeGraphSink}
 * constructor) is what makes it correct on restart and for multi-table sinks:
 *
 * <ul>
 *   <li><b>Restart</b>: on checkpoint restore the engine calls only {@link
 *       #handleSchemaSaveModeWithRestore()}, so data is never dropped a second time — previously
 *       the constructor re-ran the drop on every restart and lost data written before the restart.
 *   <li><b>Multi-table</b>: each table's sink gets its own handler and drops only the labels that
 *       table targets ({@link #handleDataSaveMode()}), so dropping table A no longer wipes table B
 *       — the old whole-graph {@code clearGraph} cleared everything (and destroyed a sibling
 *       table's freshly-created schema).
 * </ul>
 *
 * Schema work runs before the data drop (the default {@link #handleSaveMode()} order) so the labels
 * exist when their data is cleared.
 */
public class HugeGraphSaveModeHandler implements SaveModeHandler {

    private final HugeGraphSinkConfig config;
    private final SeaTunnelRowType rowType;
    private final TablePath tablePath;

    private HugeGraphClient client;

    public HugeGraphSaveModeHandler(
            HugeGraphSinkConfig config, SeaTunnelRowType rowType, TablePath tablePath) {
        this.config = config;
        this.rowType = rowType;
        this.tablePath = tablePath;
    }

    @Override
    public void open() {
        this.client = createClient();
    }

    /** Test seam: overridden in unit tests to inject a mock client instead of a live connection. */
    HugeGraphClient createClient() {
        return new HugeGraphClient(config.getConnectionConfig());
    }

    /**
     * Config-level checks (labels, idFields, MULTIPLE→sortKeys, source-field presence) run before
     * any server write so a malformed mapping cannot leave a partial schema behind — the HugeGraph
     * server is non-transactional for DDL and its primary keys / sort keys / frequency are
     * effectively immutable. Under CREATE_SCHEMA_WHEN_NOT_EXIST, missing PropertyKey / VertexLabel
     * / EdgeLabel are then auto-created and finally re-validated against the server. Under
     * ERROR_WHEN_SCHEMA_NOT_EXIST, only validation runs.
     */
    @Override
    public void handleSchemaSaveMode() {
        SchemaValidator validator = new SchemaValidator(client, rowType);
        validator.validateConfigOnly(config.getMappings());

        if (config.getSchemaSaveMode() == HugeGraphSchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST) {
            // Fail fast on any already-existing label whose immutable attributes (PK, frequency,
            // sort keys, endpoints) conflict with the config, BEFORE creating anything — a conflict
            // discovered only by the post-create validate() below would leave the PropertyKeys /
            // labels created for the other mappings behind as schema pollution.
            validator.validateExistingLabels(config.getMappings());

            SchemaManager schemaManager =
                    new SchemaManager(client, config.getSchemaSaveMode(), rowType);
            schemaManager.ensureSchema(config.getMappings());
        }

        validator.validate(config.getMappings());
    }

    /**
     * For DROP_DATA, clear only the data of the labels this job's mappings target — edges first (so
     * edge-only mappings are handled even when their endpoints are out of scope), then vertices
     * (removing a vertex also removes its remaining incident edges). Schema is preserved.
     * APPEND_DATA is a no-op.
     *
     * <p>Before deleting vertices, a pre-flight check discovers every edge label that references a
     * target vertex label. If any of those edge labels are NOT in this job's mappings, the job
     * fails fast — deleting the vertices would cascade-delete those edges silently. Set {@code
     * allow_cascade_delete_unmapped_edges=true} to opt into the destructive cascade.
     */
    @Override
    public void handleDataSaveMode() {
        if (config.getDataSaveMode() != HugeGraphDataSaveMode.DROP_DATA) {
            return;
        }
        List<MappingConfig> mappings = config.getMappings();

        // Collect the set of edge labels this job explicitly targets.
        Set<String> mappedEdgeLabels = new HashSet<>();
        Set<String> mappedVertexLabels = new HashSet<>();
        for (MappingConfig mapping : mappings) {
            if (mapping.getType() == MappingConfig.LabelType.EDGE) {
                mappedEdgeLabels.add(mapping.getLabel());
            } else {
                mappedVertexLabels.add(mapping.getLabel());
            }
        }

        // Pre-flight: for each vertex label being dropped, discover edge labels that would be
        // cascade-deleted. If any are not in this job's mappings, fail fast — unless the user
        // has explicitly opted into the destructive cascade.
        if (!config.isAllowCascadeDeleteUnmappedEdges()) {
            for (String vertexLabel : mappedVertexLabels) {
                List<String> connected = client.getConnectedEdgeLabels(vertexLabel);
                for (String edgeLabel : connected) {
                    if (!mappedEdgeLabels.contains(edgeLabel)) {
                        throw new HugeGraphConnectorException(
                                HugeGraphConnectorErrorCode.ILLEGAL_CONFIG_ARGUMENT,
                                String.format(
                                        "DROP_DATA would cascade-delete edge label '%s' (connected to "
                                                + "vertex label '%s'), which is not in this job's "
                                                + "mappings. Add '%s' to your mappings to delete it "
                                                + "explicitly, or set "
                                                + "allow_cascade_delete_unmapped_edges=true to accept "
                                                + "the destructive cascade.",
                                        edgeLabel, vertexLabel, edgeLabel));
                    }
                }
            }
        }

        for (MappingConfig mapping : mappings) {
            if (mapping.getType() == MappingConfig.LabelType.EDGE) {
                client.deleteEdgesByLabel(mapping.getLabel());
            }
        }
        for (MappingConfig mapping : mappings) {
            if (mapping.getType() == MappingConfig.LabelType.VERTEX) {
                client.deleteVerticesByLabel(mapping.getLabel());
            }
        }
    }

    /**
     * Restore path: (re)ensure and validate schema only. Deliberately never drops data — the data
     * already written before the checkpoint must survive the restart.
     */
    @Override
    public void handleSchemaSaveModeWithRestore() {
        handleSchemaSaveMode();
    }

    @Override
    public SchemaSaveMode getSchemaSaveMode() {
        return config.getSchemaSaveMode() == HugeGraphSchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST
                ? SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST
                : SchemaSaveMode.ERROR_WHEN_SCHEMA_NOT_EXIST;
    }

    @Override
    public DataSaveMode getDataSaveMode() {
        return config.getDataSaveMode() == HugeGraphDataSaveMode.DROP_DATA
                ? DataSaveMode.DROP_DATA
                : DataSaveMode.APPEND_DATA;
    }

    @Override
    public TablePath getHandleTablePath() {
        return tablePath;
    }

    @Override
    public Catalog getHandleCatalog() {
        // HugeGraph has no SeaTunnel Catalog implementation; schema/data handling goes through the
        // HugeGraph client directly. The engine's SaveModeExecuteWrapper only reads name() from
        // this
        // for logging, so a lightweight stub is sufficient.
        return new HugeGraphNamedCatalog();
    }

    @Override
    public void close() {
        if (client != null) {
            client.close();
            client = null;
        }
    }

    /**
     * Minimal {@link Catalog} that exists only to satisfy {@code SaveModeExecuteWrapper}, which
     * logs {@code getHandleCatalog().name()} before running the handler. HugeGraph does all
     * schema/data work through its own client, so every catalog operation other than {@link
     * #name()} is unsupported and never invoked on the save-mode path.
     */
    private static final class HugeGraphNamedCatalog implements Catalog {

        @Override
        public String name() {
            return "HugeGraph";
        }

        @Override
        public void open() {}

        @Override
        public void close() {}

        @Override
        public String getDefaultDatabase() {
            throw unsupported();
        }

        @Override
        public boolean databaseExists(String databaseName) {
            throw unsupported();
        }

        @Override
        public List<String> listDatabases() {
            throw unsupported();
        }

        @Override
        public List<String> listTables(String databaseName) {
            throw unsupported();
        }

        @Override
        public boolean tableExists(TablePath tablePath) {
            throw unsupported();
        }

        @Override
        public CatalogTable getTable(TablePath tablePath) {
            throw unsupported();
        }

        @Override
        public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists) {
            throw unsupported();
        }

        @Override
        public void dropTable(TablePath tablePath, boolean ignoreIfNotExists) {
            throw unsupported();
        }

        @Override
        public void createDatabase(TablePath tablePath, boolean ignoreIfExists) {
            throw unsupported();
        }

        @Override
        public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists) {
            throw unsupported();
        }

        private static UnsupportedOperationException unsupported() {
            return new UnsupportedOperationException(
                    "HugeGraph does not provide a SeaTunnel Catalog; "
                            + "schema and data save modes are handled via the HugeGraph client.");
        }
    }
}
