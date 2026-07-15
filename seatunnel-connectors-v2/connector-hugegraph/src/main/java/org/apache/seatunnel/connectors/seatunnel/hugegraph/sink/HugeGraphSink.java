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

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.client.HugeGraphClient;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphOptions;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSchemaSaveMode;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.config.HugeGraphSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.SchemaManager;
import org.apache.seatunnel.connectors.seatunnel.hugegraph.utils.SchemaValidator;

import java.io.IOException;
import java.util.Optional;

public class HugeGraphSink extends AbstractSimpleSink<SeaTunnelRow, Void>
        implements SupportMultiTableSink {

    private final HugeGraphSinkConfig config;
    private final CatalogTable catalogTable;
    private final SeaTunnelRowType rowType;

    public HugeGraphSink(HugeGraphSinkConfig config, CatalogTable catalogTable) {
        this.config = config;
        this.catalogTable = catalogTable;
        this.rowType = catalogTable.getSeaTunnelRowType();

        this.config.applyLegacyFieldSelection(rowType);
        initializeSchema();
    }

    /**
     * Schema management and validation runs once at driver-side Sink initialization. Config-level
     * checks (labels, idFields, MULTIPLE→sortKeys, source-field presence in the input row) run
     * before any server write so a malformed mapping cannot leave a partial schema behind — the
     * HugeGraph server is non-transactional for DDL and its primary keys / sort keys / frequency
     * are effectively immutable, so a partially persisted schema fragment would not be fixable in
     * place. Under CREATE_SCHEMA_WHEN_NOT_EXIST, missing PropertyKey / VertexLabel / EdgeLabel are
     * then auto-created and finally re-validated against the server. Under
     * ERROR_WHEN_SCHEMA_NOT_EXIST, only validation runs.
     */
    private void initializeSchema() {
        HugeGraphClient client = new HugeGraphClient(config.getConnectionConfig());
        RuntimeException failure = null;
        try {
            SchemaValidator validator = new SchemaValidator(client, rowType);
            validator.validateConfigOnly(config.getMappings());

            if (config.getSchemaSaveMode()
                    == HugeGraphSchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST) {
                SchemaManager schemaManager =
                        new SchemaManager(client, config.getSchemaSaveMode(), rowType);
                schemaManager.ensureSchema(config.getMappings());
            }

            validator.validate(config.getMappings());
        } catch (RuntimeException e) {
            failure = e;
            throw e;
        } finally {
            try {
                client.close();
            } catch (RuntimeException closeFailure) {
                if (failure == null) {
                    throw closeFailure;
                }
                failure.addSuppressed(closeFailure);
            }
        }
    }

    @Override
    public String getPluginName() {
        return HugeGraphOptions.PLUGIN_NAME;
    }

    @Override
    public HugeGraphSinkWriter createWriter(SinkWriter.Context context) throws IOException {
        return new HugeGraphSinkWriter(config, rowType);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.ofNullable(catalogTable);
    }
}
