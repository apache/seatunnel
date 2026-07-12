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
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
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
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cassandra.config.CassandraTableConfig;
import org.apache.seatunnel.connectors.seatunnel.cassandra.exception.CassandraConnectorException;
import org.apache.seatunnel.connectors.seatunnel.cassandra.util.TypeConvertUtil;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.ColumnDefinitions;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class CassandraSource extends AbstractSingleSplitSource<SeaTunnelRow>
        implements SupportColumnProjection {

    private static final String PLUGIN_NAME = "Cassandra";

    private final CassandraParameters cassandraParameters;
    private final List<CassandraTableConfig> tableConfigs;

    public CassandraSource(CassandraParameters cassandraParameters, ReadonlyConfig config) {
        this.cassandraParameters = cassandraParameters;
        this.tableConfigs = buildTableConfigs(cassandraParameters, config);
    }

    private List<CassandraTableConfig> buildTableConfigs(
            CassandraParameters params, ReadonlyConfig config) {
        try (CqlSession session =
                CassandraClient.getCqlSessionBuilder(
                                params.getHost(),
                                params.getKeyspace(),
                                params.getUsername(),
                                params.getPassword(),
                                params.getDatacenter())
                        .build()) {

            if (config.getOptional(ConnectorCommonOptions.TABLE_CONFIGS).isPresent()) {
                List<Map<String, Object>> tableConfigMaps =
                        config.get(ConnectorCommonOptions.TABLE_CONFIGS);
                List<CassandraTableConfig> configs = new ArrayList<>();
                for (Map<String, Object> tableConfigMap : tableConfigMaps) {
                    ReadonlyConfig tableConfig = ReadonlyConfig.fromMap(tableConfigMap);
                    String cql = tableConfig.get(CassandraSourceOptions.CQL);
                    CassandraTableConfig built =
                            buildTableConfig(
                                    cql,
                                    session,
                                    params.getKeyspace(),
                                    params.getConsistencyLevel());
                    String tableId = built.getTableId();
                    boolean duplicate =
                            configs.stream().anyMatch(c -> c.getTableId().equals(tableId));
                    if (duplicate) {
                        throw new CassandraConnectorException(
                                CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT,
                                "Duplicate table found in tables_configs: " + tableId);
                    }
                    configs.add(built);
                }
                return configs;
            } else {
                String cql = config.get(CassandraSourceOptions.CQL);
                return Collections.singletonList(
                        buildTableConfig(
                                cql, session, params.getKeyspace(), params.getConsistencyLevel()));
            }
        } catch (CassandraConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new CassandraConnectorException(
                    CommonErrorCodeDeprecated.TABLE_SCHEMA_GET_FAILED,
                    "Get table schema from Cassandra source failed",
                    e);
        }
    }

    private CassandraTableConfig buildTableConfig(
            String cql,
            CqlSession session,
            String keyspace,
            com.datastax.oss.driver.api.core.ConsistencyLevel consistencyLevel) {
        ColumnDefinitions columnDefs =
                session.execute(CassandraClient.createSimpleStatement(cql, consistencyLevel))
                        .getColumnDefinitions();

        if (columnDefs.size() == 0) {
            throw new CassandraConnectorException(
                    CommonErrorCodeDeprecated.TABLE_SCHEMA_GET_FAILED,
                    "No columns returned by CQL: " + cql);
        }

        TableSchema.Builder schemaBuilder = TableSchema.builder();
        String tableName = "default";
        for (int i = 0; i < columnDefs.size(); i++) {
            schemaBuilder.column(
                    PhysicalColumn.of(
                            columnDefs.get(i).getName().asInternal(),
                            TypeConvertUtil.convert(columnDefs.get(i).getType()),
                            null,
                            null,
                            true,
                            null,
                            null));
            tableName = columnDefs.get(i).getTable().asInternal();
        }

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of(PLUGIN_NAME, keyspace, tableName),
                        schemaBuilder.build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "");

        return new CassandraTableConfig(cql, catalogTable);
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        return tableConfigs.stream()
                .map(CassandraTableConfig::getCatalogTable)
                .collect(Collectors.toList());
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) throws Exception {
        return new CassandraSourceReader(cassandraParameters, tableConfigs, readerContext);
    }
}
