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

package org.apache.seatunnel.transform.common;

import org.apache.seatunnel.api.common.CommonOptions;
import org.apache.seatunnel.api.common.PrepareFailException;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import lombok.NonNull;

import java.util.Objects;

public abstract class AbstractSeaTunnelTransform implements SeaTunnelTransform<SeaTunnelRow> {

    private static final String RESULT_TABLE_NAME = CommonOptions.RESULT_TABLE_NAME.key();
    private static final String SOURCE_TABLE_NAME = CommonOptions.SOURCE_TABLE_NAME.key();

    protected String inputTableName;
    protected SeaTunnelRowType inputRowType;

    protected String outputTableName;
    protected SeaTunnelRowType outputRowType;

    protected CatalogTable inputCatalogTable;

    protected volatile CatalogTable outputCatalogTable;

    public AbstractSeaTunnelTransform(@NonNull CatalogTable inputCatalogTable) {
        this.inputCatalogTable = inputCatalogTable;
    }

    @Override
    public void prepare(Config pluginConfig) throws PrepareFailException {
        if (!pluginConfig.hasPath(SOURCE_TABLE_NAME)) {
            throw new IllegalArgumentException(
                "The configuration missing key: " + SOURCE_TABLE_NAME);
        }
        if (!pluginConfig.hasPath(RESULT_TABLE_NAME)) {
            throw new IllegalArgumentException(
                "The configuration missing key: " + RESULT_TABLE_NAME);
        }

        this.inputTableName = pluginConfig.getString(SOURCE_TABLE_NAME);
        this.outputTableName = pluginConfig.getString(RESULT_TABLE_NAME);
        if (Objects.equals(inputTableName, outputTableName)) {
            throw new IllegalArgumentException(
                "source and result cannot be equals: "
                    + inputTableName
                    + ", "
                    + outputTableName);
        }

        setConfig(pluginConfig);
    }

    @Override
    public void setTypeInfo(SeaTunnelDataType<SeaTunnelRow> inputDataType) {
        this.inputRowType = (SeaTunnelRowType) inputDataType;
        this.outputRowType = transformRowType(clone(inputRowType));
    }

    @Override
    public SeaTunnelRow map(SeaTunnelRow row) {
        return transformRow(row);
    }

    protected abstract void setConfig(Config pluginConfig);

    /**
     * Outputs transformed row type.
     *
     * @param inputRowType upstream input row type
     * @return
     */
    protected abstract SeaTunnelRowType transformRowType(SeaTunnelRowType inputRowType);

    /**
     * Outputs transformed row data.
     *
     * @param inputRow upstream input row data
     * @return
     */
    protected abstract SeaTunnelRow transformRow(SeaTunnelRow inputRow);

    private static SeaTunnelRowType clone(SeaTunnelRowType rowType) {
        String[] fieldNames = new String[rowType.getTotalFields()];
        System.arraycopy(rowType.getFieldNames(), 0, fieldNames, 0, fieldNames.length);

        SeaTunnelDataType[] fieldTypes = new SeaTunnelDataType[rowType.getTotalFields()];
        System.arraycopy(rowType.getFieldTypes(), 0, fieldTypes, 0, fieldTypes.length);

        return new SeaTunnelRowType(fieldNames, fieldTypes);
    }

    @Override
    public CatalogTable getProducedCatalogTable() {
        if (outputCatalogTable == null) {
            synchronized (this) {
                outputCatalogTable = transformCatalogTable();
            }
        }

        return outputCatalogTable;
    }

    private CatalogTable transformCatalogTable() {
        TableIdentifier tableIdentifier = transformTableIdentifier();
        TableSchema tableSchema = transformTableSchema();
        CatalogTable catalogTable = CatalogTable.of(tableIdentifier, tableSchema, inputCatalogTable.getOptions(),
            inputCatalogTable.getPartitionKeys(), inputCatalogTable.getComment());
        return catalogTable;
    }

    protected abstract TableSchema transformTableSchema();

    protected abstract TableIdentifier transformTableIdentifier();
}
