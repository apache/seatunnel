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

package org.apache.seatunnel.transform.adaptsink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.transform.adaptsink.DefineSinkTypeTransformConfig.DefineColumnType;
import org.apache.seatunnel.transform.common.TransformCommonOptions;

import com.google.auto.service.AutoService;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@AutoService(Factory.class)
public class DefineSinkTypeTransformFactory implements TableTransformFactory {
    @Override
    public String factoryIdentifier() {
        return DefineSinkTypeTransformConfig.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        DefineSinkTypeTransformConfig.COLUMNS,
                        Conditions.notEmpty(DefineSinkTypeTransformConfig.COLUMNS)
                                .and(
                                        Conditions.extension(
                                                DefineSinkTypeTransformConfig.COLUMNS,
                                                new ColumnsStructureValidator())))
                .optional(TransformCommonOptions.MULTI_TABLES)
                .optional(TransformCommonOptions.TABLE_MATCH_REGEX)
                .build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        return () ->
                new DefineSinkTypeMultiCatalogTransform(
                        context.getCatalogTables(), context.getOptions());
    }

    static class ColumnsStructureValidator implements ConditionExtension<List<DefineColumnType>> {
        @Override
        public String description() {
            return "each column entry must contain non-null 'column' and 'type'";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, List<DefineColumnType> value)
                throws OptionValidationException {
            if (value == null) {
                return false;
            }
            Set<String> seen = new HashSet<>();
            for (int i = 0; i < value.size(); i++) {
                DefineColumnType entry = value.get(i);
                if (entry.getColumn() == null || entry.getColumn().trim().isEmpty()) {
                    throw new OptionValidationException(
                            String.format(
                                    "columns[%d]: 'column' name must not be null or empty", i));
                }
                if (entry.getType() == null || entry.getType().trim().isEmpty()) {
                    throw new OptionValidationException(
                            String.format("columns[%d]: 'type' must not be null or empty", i));
                }
                if (!seen.add(entry.getColumn())) {
                    throw new OptionValidationException(
                            String.format(
                                    "columns[%d]: duplicate column name '%s'",
                                    i, entry.getColumn()));
                }
            }
            return true;
        }
    }
}
