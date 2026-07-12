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

package org.apache.seatunnel.transform.chunk;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.transform.common.TransformCommonOptions;

import com.google.auto.service.AutoService;

import java.util.List;
import java.util.Map;

@AutoService(Factory.class)
public class TextChunkTransformFactory implements TableTransformFactory {

    @Override
    public String factoryIdentifier() {
        return TextChunkTransform.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        TextChunkTransformConfig.TEXT_FIELD,
                        Conditions.notBlank(TextChunkTransformConfig.TEXT_FIELD),
                        // Anchored on TEXT_FIELD (the only absolutely-required option) on purpose:
                        // a value constraint only runs when its head option is required or
                        // explicitly present, so anchoring on the optional output_field/
                        // chunk_index_field would skip the check when the user relies on their
                        // defaults -- exactly the default-collision case this validator must catch.
                        Conditions.extension(
                                TextChunkTransformConfig.TEXT_FIELD,
                                new OutputFieldNamesValidator()))
                .optional(
                        TextChunkTransformConfig.OUTPUT_FIELD,
                        TextChunkTransformConfig.CHUNK_INDEX_FIELD,
                        TextChunkTransformConfig.SEPARATORS)
                .optional(
                        TextChunkTransformConfig.CHUNK_SIZE,
                        Conditions.greaterThan(TextChunkTransformConfig.CHUNK_SIZE, 0))
                .optional(
                        TextChunkTransformConfig.OVERLAP_SIZE,
                        Conditions.greaterOrEqual(TextChunkTransformConfig.OVERLAP_SIZE, 0),
                        Conditions.lessThanField(
                                TextChunkTransformConfig.OVERLAP_SIZE,
                                TextChunkTransformConfig.CHUNK_SIZE))
                .optional(
                        TransformCommonOptions.MULTI_TABLES,
                        Conditions.extension(
                                TransformCommonOptions.MULTI_TABLES,
                                new TableTransformRulesValidator()))
                .optional(TransformCommonOptions.TABLE_MATCH_REGEX)
                .build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        return () ->
                new TextChunkMultiCatalogTransform(
                        context.getCatalogTables(), context.getOptions());
    }

    static class OutputFieldNamesValidator implements ConditionExtension<String> {

        @Override
        public String description() {
            return "output_field and chunk_index_field must be different";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, String textField) {
            return !config.get(TextChunkTransformConfig.OUTPUT_FIELD)
                    .equals(config.get(TextChunkTransformConfig.CHUNK_INDEX_FIELD));
        }
    }

    static class TableTransformRulesValidator
            implements ConditionExtension<List<Map<String, Object>>> {

        @Override
        public String description() {
            return "each table_transform entry must satisfy chunk_size > 0, "
                    + "0 <= overlap_size < chunk_size, and output_field != chunk_index_field";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, List<Map<String, Object>> tables) {
            if (tables == null) {
                return true;
            }
            for (Map<String, Object> table : tables) {
                TextChunkTransformConfig.validate(
                        TextChunkTransformConfig.of(ReadonlyConfig.fromMap(table)));
            }
            return true;
        }
    }
}
