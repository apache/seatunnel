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

package org.apache.seatunnel.connectors.seatunnel.iceberg;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.iceberg.config.IcebergCommonConfig;

import org.apache.commons.lang3.StringUtils;
import org.apache.iceberg.CachingCatalog;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.Objects;

@Slf4j
public class IcebergTableLoader implements Closeable, Serializable {

    private static final long serialVersionUID = 9061073826700804273L;

    private final IcebergCatalogLoader icebergCatalogFactory;
    private final String tableIdentifierStr;
    private transient Catalog catalog;

    public IcebergTableLoader(
            @NonNull IcebergCatalogLoader icebergCatalogFactory,
            @NonNull TableIdentifier tableIdentifier) {
        this.icebergCatalogFactory = icebergCatalogFactory;
        this.tableIdentifierStr = tableIdentifier.toString();
    }

    public Catalog getCatalog() {
        return catalog;
    }

    public TableIdentifier getTableIdentifier() {
        return TableIdentifier.parse(tableIdentifierStr);
    }

    public IcebergTableLoader open() {
        catalog = CachingCatalog.wrap(icebergCatalogFactory.loadCatalog());
        return this;
    }

    public Table loadTable() {
        TableIdentifier tableIdentifier = TableIdentifier.parse(tableIdentifierStr);
        if (catalog == null) {
            open();
        }
        return catalog.loadTable(tableIdentifier);
    }

    @Override
    public void close() throws IOException {
        if (catalog != null && catalog instanceof Closeable) {
            ((Closeable) catalog).close();
        }
    }

    @VisibleForTesting
    public static IcebergTableLoader create(IcebergCommonConfig config) {
        return create(config, null);
    }

    public static IcebergTableLoader create(IcebergCommonConfig config, CatalogTable catalogTable) {
        IcebergCatalogLoader catalogFactory = new IcebergCatalogLoader(config);
        String table;
        if (Objects.nonNull(catalogTable)
                && StringUtils.isNotEmpty(catalogTable.getTableId().getTableName())) {
            log.info(
                    "Config table name is empty, use catalog table name: {}",
                    catalogTable.getTableId().getTableName());
            table = catalogTable.getTableId().getTableName();
        } else if (StringUtils.isNotEmpty(config.getTable())) {
            // for test in sink
            table = config.getTable();
        } else {
            throw new IllegalArgumentException("Table name is empty");
        }
        return new IcebergTableLoader(
                catalogFactory, TableIdentifier.of(Namespace.of(config.getNamespace()), table));
    }
}
