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

package org.apache.seatunnel.transform.jsonpath;

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

@AutoService(Factory.class)
public class JsonPathTransformFactory implements TableTransformFactory {
    @Override
    public String factoryIdentifier() {
        return "JsonPath";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        JsonPathTransformConfig.COLUMNS,
                        Conditions.notEmpty(JsonPathTransformConfig.COLUMNS)
                                .and(
                                        Conditions.extension(
                                                JsonPathTransformConfig.COLUMNS,
                                                new ColumnsValidator())))
                .optional(TransformCommonOptions.MULTI_TABLES)
                .optional(TransformCommonOptions.TABLE_MATCH_REGEX)
                .optional(TransformCommonOptions.ROW_ERROR_HANDLE_WAY_OPTION)
                .build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        return () ->
                new JsonPathMultiCatalogTransform(context.getCatalogTables(), context.getOptions());
    }

    static class ColumnsValidator implements ConditionExtension<List<Map<String, Object>>> {
        @Override
        public String description() {
            return "each column entry must contain non-empty 'path' and 'dest_field'";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, List<Map<String, Object>> value)
                throws OptionValidationException {
            if (value == null) {
                return false;
            }
            for (int i = 0; i < value.size(); i++) {
                Map<String, Object> entry = value.get(i);
                Object path = entry.get("path");
                if (path == null
                        || (path instanceof String && ((String) path).trim().isEmpty())
                        || (path instanceof List && ((List<?>) path).isEmpty())) {
                    throw new OptionValidationException(
                            String.format("columns[%d]: 'path' must not be null or empty", i));
                }
                Object srcField = entry.get("src_field");
                if (srcField == null
                        || (srcField instanceof String && ((String) srcField).trim().isEmpty())) {
                    throw new OptionValidationException(
                            String.format("columns[%d]: 'src_field' must not be null or empty", i));
                }
                Object destField = entry.get("dest_field");
                if (destField == null
                        || (destField instanceof String && ((String) destField).trim().isEmpty())
                        || (destField instanceof List && ((List<?>) destField).isEmpty())) {
                    throw new OptionValidationException(
                            String.format(
                                    "columns[%d]: 'dest_field' must not be null or empty", i));
                }
            }
            return true;
        }
    }
}
