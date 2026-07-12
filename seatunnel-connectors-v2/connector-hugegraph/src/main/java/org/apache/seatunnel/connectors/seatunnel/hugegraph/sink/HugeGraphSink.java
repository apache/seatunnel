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

public class HugeGraphSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

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
     * Schema management and validation runs once at driver-side Sink initialization. Under
     * CREATE_SCHEMA_WHEN_NOT_EXIST, missing PropertyKey/VertexLabel/EdgeLabel are auto-created
     * first, then validated. Under ERROR_WHEN_SCHEMA_NOT_EXIST, only validation runs.
     */
    private void initializeSchema() {
        HugeGraphClient client = new HugeGraphClient(config.getConnectionConfig());
        RuntimeException failure = null;
        try {
            if (config.getSchemaSaveMode()
                    == HugeGraphSchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST) {
                SchemaManager schemaManager =
                        new SchemaManager(client, config.getSchemaSaveMode(), rowType);
                schemaManager.ensureSchema(config.getMappings());
            }

            SchemaValidator validator = new SchemaValidator(client, rowType);
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
