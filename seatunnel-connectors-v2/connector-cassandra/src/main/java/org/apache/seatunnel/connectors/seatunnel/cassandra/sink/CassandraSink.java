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

package org.apache.seatunnel.connectors.seatunnel.cassandra.sink;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.connectors.seatunnel.cassandra.client.CassandraClient;
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraParameters;
import org.apache.seatunnel.connectors.seatunnel.cassandra.exception.CassandraConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.cassandra.exception.CassandraConnectorException;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSimpleSink;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSinkOptions.TABLE;

public class CassandraSink extends AbstractSimpleSink<SeaTunnelRow, Void> {

    private final CassandraParameters cassandraParameters;
    private final CatalogTable catalogTable;
    private final ColumnDefinitions tableSchema;

    public CassandraSink(
            CassandraParameters cassandraParameters,
            CatalogTable catalogTable,
            ReadonlyConfig pluginConfig) {
        this.cassandraParameters = cassandraParameters;
        this.catalogTable = catalogTable;
        try (CqlSession session =
                CassandraClient.getCqlSessionBuilder(
                                cassandraParameters.getHost(),
                                cassandraParameters.getKeyspace(),
                                cassandraParameters.getUsername(),
                                cassandraParameters.getPassword(),
                                cassandraParameters.getDatacenter())
                        .build()) {
            List<String> fields = cassandraParameters.getFields();
            this.tableSchema = CassandraClient.getTableSchema(session, pluginConfig.get(TABLE));
            if (fields == null || fields.isEmpty()) {
                List<String> newFields = new ArrayList<>();
                for (int i = 0; i < tableSchema.size(); i++) {
                    newFields.add(tableSchema.get(i).getName().asInternal());
                }
                this.cassandraParameters.setFields(newFields);
            } else {
                for (String field : fields) {
                    if (!tableSchema.contains(field)) {
                        throw new CassandraConnectorException(
                                CassandraConnectorErrorCode.FIELD_NOT_IN_TABLE,
                                "Field "
                                        + field
                                        + " does not exist in table "
                                        + pluginConfig.get(TABLE));
                    }
                }
            }
        } catch (Exception e) {
            throw new CassandraConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            getPluginName(), PluginType.SINK, ExceptionUtils.getMessage(e)));
        }
    }

    @Override
    public String getPluginName() {
        return "Cassandra";
    }

    @Override
    public AbstractSinkWriter<SeaTunnelRow, Void> createWriter(SinkWriter.Context context)
            throws IOException {
        return new CassandraSinkWriter(
                cassandraParameters, catalogTable.getSeaTunnelRowType(), tableSchema);
    }

    @Override
    public Optional<CatalogTable> getWriteCatalogTable() {
        return Optional.of(catalogTable);
    }
}
