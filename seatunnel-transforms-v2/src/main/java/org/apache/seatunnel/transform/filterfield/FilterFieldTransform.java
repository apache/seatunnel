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

package org.apache.seatunnel.transform.filterfield;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;
import org.apache.seatunnel.transform.common.AbstractSeaTunnelTransform;
import org.apache.seatunnel.transform.copyfield.CopyFieldTransformConfig;
import org.apache.seatunnel.transform.copyfield.CopyFieldTransformFactory;

import com.google.auto.service.AutoService;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@AutoService(SeaTunnelTransform.class)
public class FilterFieldTransform extends AbstractSeaTunnelTransform {
    private int[] inputValueIndex;
    private FilterFieldTransformConfig config;

    public FilterFieldTransform(@NonNull FilterFieldTransformConfig config,  @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return "Filter";
    }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        // todo reuse array container if not remove fields
        String[] fields = this.config.getFields();
        Object[] values = new Object[fields.length];
        for (int i = 0; i < fields.length; i++) {
            values[i] = inputRow.getField(inputValueIndex[i]);
        }
        return new SeaTunnelRow(values);
    }

    @Override
    protected TableSchema transformTableSchema() {
        List<String> filterFields = Arrays.asList(this.config.getFields());
        List<Column> outputColumns = new ArrayList<>();

        SeaTunnelRowType seaTunnelRowType = inputCatalogTable.getTableSchema().toPhysicalRowDataType();

        inputValueIndex = new int[filterFields.size()];
        for (int i = 0; i < filterFields.size(); i++) {
            String field = filterFields.get(i);
            int inputFieldIndex = seaTunnelRowType.indexOf(field);
            if (inputFieldIndex == -1) {
                throw new IllegalArgumentException(
                    "Cannot find [" + field + "] field in input row type");
            }
            inputValueIndex[i] = inputFieldIndex;
            outputColumns.add(inputCatalogTable.getTableSchema().getColumns().get(inputFieldIndex).copy());
        }

        return TableSchema.builder()
            .columns(outputColumns)
            .primaryKey(inputCatalogTable.getTableSchema().getPrimaryKey())
            .constraintKey(inputCatalogTable.getTableSchema().getConstraintKeys())
            .build();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        return inputCatalogTable.getTableId();
    }

    @Override
    public SeaTunnelDataType<SeaTunnelRow> getProducedType() {
        return this.outputCatalogTable.getTableSchema().toPhysicalRowDataType();
    }
}
