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

package org.apache.seatunnel.connectors.seatunnel.cassandra.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SupportColumnProjection;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.cassandra.client.CassandraClient;
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraParameters;
import org.apache.seatunnel.connectors.seatunnel.cassandra.exception.CassandraConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.cassandra.exception.CassandraConnectorException;
import org.apache.seatunnel.connectors.seatunnel.cassandra.util.TypeConvertUtil;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.Row;

import java.util.Collections;
import java.util.List;

import static org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSourceOptions.CQL;

public class CassandraSource extends AbstractSingleSplitSource<SeaTunnelRow>
        implements SupportColumnProjection {

    private final CassandraParameters cassandraParameters;
    private final CatalogTable catalogTable;

    public CassandraSource(CassandraParameters cassandraParameters, ReadonlyConfig pluginConfig) {
        this.cassandraParameters = cassandraParameters;

        try (CqlSession currentSession =
                CassandraClient.getCqlSessionBuilder(
                                cassandraParameters.getHost(),
                                cassandraParameters.getKeyspace(),
                                cassandraParameters.getUsername(),
                                cassandraParameters.getPassword(),
                                cassandraParameters.getDatacenter())
                        .build()) {
            Row rs =
                    currentSession
                            .execute(
                                    CassandraClient.createSimpleStatement(
                                            pluginConfig.get(CQL),
                                            cassandraParameters.getConsistencyLevel()))
                            .one();
            if (rs == null) {
                throw new CassandraConnectorException(
                        CassandraConnectorErrorCode.NO_DATA_IN_SOURCE_TABLE,
                        "No data select from this cql: " + pluginConfig.get(CQL));
            }
            int columnSize = rs.getColumnDefinitions().size();
            TableSchema.Builder schemaBuilder = TableSchema.builder();
            String tableName = "default";
            for (int i = 0; i < columnSize; i++) {
                PhysicalColumn physicalColumn =
                        PhysicalColumn.of(
                                rs.getColumnDefinitions().get(i).getName().asInternal(),
                                TypeConvertUtil.convert(rs.getColumnDefinitions().get(i).getType()),
                                null,
                                null,
                                true,
                                null,
                                null);
                schemaBuilder.column(physicalColumn);
                tableName = rs.getColumnDefinitions().get(i).getTable().asInternal();
            }
            catalogTable =
                    CatalogTable.of(
                            TableIdentifier.of(
                                    getPluginName(), cassandraParameters.getKeyspace(), tableName),
                            schemaBuilder.build(),
                            Collections.emptyMap(),
                            Collections.emptyList(),
                            "");
        } catch (Exception e) {
            throw new CassandraConnectorException(
                    CommonErrorCodeDeprecated.TABLE_SCHEMA_GET_FAILED,
                    "Get table schema from cassandra source data failed",
                    e);
        }
    }

    @Override
    public String getPluginName() {
        return "Cassandra";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return Collections.singletonList(catalogTable);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new CassandraSourceReader(cassandraParameters, readerContext);
    }
}
