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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.DefaultSaveModeHandler;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.catalog.MaxComputeCatalog;

import org.apache.commons.lang3.StringUtils;

import com.aliyun.odps.PartitionSpec;

import static org.apache.seatunnel.connectors.seatunnel.maxcompute.config.MaxcomputeConfig.PARTITION_SPEC;

public class MaxComputeSaveModeHandler extends DefaultSaveModeHandler {

    private final ReadonlyConfig readonlyConfig;

    public MaxComputeSaveModeHandler(
            SchemaSaveMode schemaSaveMode,
            DataSaveMode dataSaveMode,
            Catalog catalog,
            CatalogTable catalogTable,
            String customSql,
            ReadonlyConfig readonlyConfig) {
        super(schemaSaveMode, dataSaveMode, catalog, catalogTable, customSql);
        this.readonlyConfig = readonlyConfig;
    }

    @Override
    protected void createSchemaWhenNotExist() {
        super.createSchemaWhenNotExist();
        if (StringUtils.isNotEmpty(readonlyConfig.get(PARTITION_SPEC))) {
            ((MaxComputeCatalog) catalog)
                    .createPartition(
                            tablePath, new PartitionSpec(readonlyConfig.get(PARTITION_SPEC)));
        }
    }

    @Override
    protected void recreateSchema() {
        super.recreateSchema();
        if (StringUtils.isNotEmpty(readonlyConfig.get(PARTITION_SPEC))) {
            ((MaxComputeCatalog) catalog)
                    .createPartition(
                            tablePath, new PartitionSpec(readonlyConfig.get(PARTITION_SPEC)));
        }
    }
}
