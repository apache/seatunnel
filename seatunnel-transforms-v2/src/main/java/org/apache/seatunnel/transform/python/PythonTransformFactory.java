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

package org.apache.seatunnel.transform.python;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.transform.common.TransformCommonOptions;

import com.google.auto.service.AutoService;

import java.util.List;
import java.util.Map;

/** Factory that exposes the Python transform through SeaTunnel's table transform SPI. */
@AutoService(Factory.class)
public class PythonTransformFactory implements TableTransformFactory {

    /**
     * Returns the user-facing plugin identifier.
     *
     * @return plugin name used in job configs
     */
    @Override
    public String factoryIdentifier() {
        return PythonTransform.PLUGIN_NAME;
    }

    /**
     * Declares the configuration contract for the Python transform.
     *
     * @return validation rules for runtime options
     */
    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        PythonTransformConfig.COLUMNS,
                        Conditions.notEmpty(PythonTransformConfig.COLUMNS)
                                .and(
                                        Conditions.extension(
                                                PythonTransformConfig.COLUMNS,
                                                new ColumnsValidator())))
                .optional(PythonTransformConfig.SOURCE_CODE)
                .optional(PythonTransformConfig.SOURCE_CODE_PATH)
                .optional(PythonTransformConfig.PYTHON_EXECUTABLE)
                .optional(PythonTransformConfig.SCRIPT_CONFIG)
                .optional(TransformCommonOptions.MULTI_TABLES)
                .optional(TransformCommonOptions.TABLE_MATCH_REGEX)
                .optional(TransformCommonOptions.RULE_MATCH_MODE)
                .optional(TransformCommonOptions.ROW_ERROR_HANDLE_WAY_OPTION)
                .build();
    }

    /**
     * Creates a multi-table aware transform wrapper for the current pipeline.
     *
     * @param context transform creation context
     * @return lazy transform supplier
     */
    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        return () ->
                new PythonMultiCatalogTransform(context.getCatalogTables(), context.getOptions());
    }

    /** Validates that each declared output column contains a target field name. */
    static class ColumnsValidator implements ConditionExtension<List<Map<String, String>>> {
        /**
         * Returns the validation error description surfaced to users.
         *
         * @return validation description
         */
        @Override
        public String description() {
            return "each column entry must contain a non-empty 'dest_field'";
        }

        /**
         * Checks every output column entry before transform construction.
         *
         * @param config readonly transform config
         * @param value raw columns option value
         * @return true when every column entry is valid
         * @throws OptionValidationException when a malformed column entry is found
         */
        @Override
        public boolean evaluate(ReadonlyConfig config, List<Map<String, String>> value)
                throws OptionValidationException {
            if (value == null || value.isEmpty()) {
                return false;
            }
            for (int i = 0; i < value.size(); i++) {
                Map<String, String> entry = value.get(i);
                String destField = entry.get(PythonTransformConfig.DEST_FIELD.key());
                if (destField == null || destField.trim().isEmpty()) {
                    throw new OptionValidationException(
                            String.format(
                                    "columns[%d]: 'dest_field' must not be null or empty", i));
                }
            }
            return true;
        }
    }
}
