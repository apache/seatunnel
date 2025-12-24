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

package org.apache.seatunnel.transform.table;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportMapTransform;

public class TableMergeTransform extends AbstractCatalogSupportMapTransform {
    public static String PLUGIN_NAME = "TableMerge";

    private final CatalogTable inputTable;
    private final TablePath outputTablePath;
    private final String outputTableId;

    public TableMergeTransform(TableMergeConfig config, CatalogTable table) {
        super(table);
        this.inputTable = table;
        this.outputTablePath = config.getTablePath();
        this.outputTableId = config.getTablePath().getFullName();
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    protected TableSchema transformTableSchema() {
        return inputTable.getTableSchema();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return TableIdentifier.of(inputTable.getTableId().getCatalogName(), outputTablePath);
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        if (inputRow.getTableId() == null || !outputTableId.equals(inputRow.getTableId())) {
            inputRow.setTableId(outputTableId);
        }
        return inputRow;
    }
}
