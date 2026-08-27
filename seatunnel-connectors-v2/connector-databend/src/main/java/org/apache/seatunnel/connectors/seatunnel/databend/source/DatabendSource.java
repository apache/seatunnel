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

package org.apache.seatunnel.connectors.seatunnel.databend.source;

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitSource;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendSourceConfig;

import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;

@Slf4j
public class DatabendSource extends AbstractSingleSplitSource<SeaTunnelRow> {

    private final CatalogTable catalogTable;
    private final String sql;
    private final String jdbcUrl;
    private final Boolean ssl;
    private final String username;
    private final String password;
    private final Integer fetchSize;
    private SeaTunnelRowType rowTypeInfo;
    private DatabendSourceReader reader;

    public DatabendSource(
            CatalogTable catalogTable,
            String sql,
            String url,
            Boolean ssl,
            String username,
            String password,
            Integer fetchSize) {
        Objects.requireNonNull(catalogTable, "catalogTable cannot be null");
        Objects.requireNonNull(url, "jdbcUrl cannot be null");
        log.info("sjh-Databend jdbcUrl: {}", url);

        this.catalogTable = catalogTable;
        this.sql = sql;
        this.jdbcUrl = url;
        this.ssl = ssl;
        this.username = username;
        this.password = password;
        this.fetchSize = fetchSize;
        this.rowTypeInfo = catalogTable.getSeaTunnelRowType();
    }

    @Override
    public String getPluginName() {
        return "Databend";
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.BOUNDED;
    }

    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        if (reader != null && reader.getRowType() != null) {
            TableSchema.Builder builder = TableSchema.builder();
            SeaTunnelRowType inferredRowType = reader.getRowType();
            for (int i = 0; i < inferredRowType.getFieldNames().length; i++) {
                Column column =
                        PhysicalColumn.builder()
                                .name(inferredRowType.getFieldNames()[i])
                                .dataType(inferredRowType.getFieldTypes()[i])
                                .nullable(true)
                                .build();
                builder.column(column);
            }
            TableSchema tableSchema = builder.build();

            CatalogTable updatedCatalogTable =
                    CatalogTable.of(
                            catalogTable.getTableId(),
                            tableSchema,
                            catalogTable.getOptions(),
                            catalogTable.getPartitionKeys(),
                            catalogTable.getComment(),
                            catalogTable.getCatalogName());

            return Collections.singletonList(updatedCatalogTable);
        }
        return Collections.singletonList(catalogTable);
    }

    @Override
    public AbstractSingleSplitReader<SeaTunnelRow> createReader(
            SingleSplitReaderContext readerContext) {
        // create a DatabendSourceConfig
        DatabendSourceConfig sourceConfig = new DatabendSourceConfig();
        sourceConfig.setUrl(jdbcUrl);
        sourceConfig.setUsername(username);
        sourceConfig.setPassword(password);
        sourceConfig.setSsl(ssl);
        sourceConfig.setFetchSize(fetchSize);

        // create properties
        Properties properties = new Properties();
        properties.setProperty("user", username);
        properties.setProperty("password", password);
        if (ssl != null) {
            properties.setProperty("ssl", ssl.toString());
        }
        sourceConfig.setProperties(properties);

        reader = new DatabendSourceReader(readerContext, sourceConfig, sql, rowTypeInfo);
        return reader;
    }
}
