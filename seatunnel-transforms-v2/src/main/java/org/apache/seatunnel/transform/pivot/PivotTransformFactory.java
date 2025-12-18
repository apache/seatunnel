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

package org.apache.seatunnel.transform.pivot;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.transform.common.TransformCommonOptions;

import com.google.auto.service.AutoService;

import java.util.List;

/**
 * Factory for creating PivotTransform instances.
 *
 * <p>This factory is discovered via SPI mechanism using the {@link AutoService} annotation.
 *
 * <p>Usage example in configuration:
 *
 * <pre>
 * transform {
 *   Pivot {
 *     group_by_keys = ["id"]
 *     pivot_column = "type"
 *     value_column = "value"
 *     pivot_values = ["A", "B", "C"]
 *   }
 * }
 * </pre>
 */
@AutoService(Factory.class)
public class PivotTransformFactory implements TableTransformFactory {

    @Override
    public String factoryIdentifier() {
        return PivotTransformConfig.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        PivotTransformConfig.GROUP_BY_KEYS,
                        PivotTransformConfig.PIVOT_COLUMN,
                        PivotTransformConfig.VALUE_COLUMN,
                        PivotTransformConfig.PIVOT_VALUES)
                .optional(
                        PivotTransformConfig.DEFAULT_VALUE,
                        PivotTransformConfig.MAX_BUFFER_SIZE,
                        PivotTransformConfig.GROUP_TIMEOUT_MS,
                        TransformCommonOptions.MULTI_TABLES,
                        TransformCommonOptions.TABLE_MATCH_REGEX)
                .build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        List<CatalogTable> catalogTables = context.getCatalogTables();

        // For simplicity, we only support single table for now
        // Multi-table support can be added later using AbstractMultiCatalogTransform
        if (catalogTables.isEmpty()) {
            throw new IllegalArgumentException("No input catalog table provided");
        }

        CatalogTable inputTable = catalogTables.get(0);
        return () -> new PivotTransform(inputTable, context.getOptions());
    }
}
