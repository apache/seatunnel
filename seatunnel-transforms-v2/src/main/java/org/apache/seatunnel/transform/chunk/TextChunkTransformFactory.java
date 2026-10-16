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
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.transform.common.TableTransformRuleValidator;
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
        // The rule of a single table: the text field to split plus the optional chunking options.
        OptionRule tableRule =
                OptionRule.builder()
                        .required(
                                TextChunkTransformConfig.TEXT_FIELD,
                                Conditions.notBlank(TextChunkTransformConfig.TEXT_FIELD),
                                // Anchored on TEXT_FIELD: a constraint anchored on the optional
                                // fields would be skipped when they rely on their defaults.
                                Conditions.extension(
                                        TextChunkTransformConfig.TEXT_FIELD,
                                        new OutputFieldNamesValidator()))
                        .optional(
                                TextChunkTransformConfig.OUTPUT_FIELD,
                                TextChunkTransformConfig.CHUNK_INDEX_FIELD,
                                TextChunkTransformConfig.SEPARATORS,
                                TextChunkTransformConfig.SKIP_EMPTY_TEXT)
                        .optional(
                                TextChunkTransformConfig.CHUNK_SIZE,
                                Conditions.greaterThan(TextChunkTransformConfig.CHUNK_SIZE, 0))
                        .optional(
                                TextChunkTransformConfig.OVERLAP_SIZE,
                                Conditions.greaterOrEqual(TextChunkTransformConfig.OVERLAP_SIZE, 0),
                                Conditions.lessThanField(
                                        TextChunkTransformConfig.OVERLAP_SIZE,
                                        TextChunkTransformConfig.CHUNK_SIZE))
                        .build();
        return OptionRule.builder()
                .optional(
                        TextChunkTransformConfig.TEXT_FIELD,
                        TextChunkTransformConfig.OUTPUT_FIELD,
                        TextChunkTransformConfig.CHUNK_INDEX_FIELD,
                        TextChunkTransformConfig.SEPARATORS,
                        TextChunkTransformConfig.SKIP_EMPTY_TEXT,
                        TextChunkTransformConfig.CHUNK_SIZE,
                        TextChunkTransformConfig.OVERLAP_SIZE)
                .optional(
                        TransformCommonOptions.MULTI_TABLES,
                        Conditions.extension(
                                TransformCommonOptions.MULTI_TABLES,
                                new TableTransformRuleValidator(tableRule)),
                        Conditions.extension(
                                TransformCommonOptions.MULTI_TABLES,
                                new DefaultAwareEntryValidator()))
                .optional(TransformCommonOptions.TABLE_MATCH_REGEX)
                .conditionalRule(
                        TransformCommonOptions.MULTI_TABLES,
                        TransformCommonOptions.MULTI_TABLES.defaultValue(),
                        tableRule)
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

    /**
     * Validates every {@code table_transform} entry with all default values applied.
     *
     * <p>The cross-field constraints of the option rule only run when both of their options are
     * explicitly configured, so an entry that relies on the default {@code chunk_size} would
     * otherwise skip the {@code overlap_size < chunk_size} check.
     */
    static class DefaultAwareEntryValidator
            implements ConditionExtension<List<Map<String, Object>>> {

        @Override
        public String description() {
            return "each table_transform entry must satisfy chunk_size > 0 and "
                    + "0 <= overlap_size < chunk_size with default values applied";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, List<Map<String, Object>> tableTransforms) {
            if (tableTransforms == null) {
                return true;
            }
            for (Map<String, Object> tableTransform : tableTransforms) {
                validate(TextChunkTransformConfig.of(ReadonlyConfig.fromMap(tableTransform)));
            }
            return true;
        }

        private void validate(TextChunkTransformConfig config) {
            if (config.getChunkSize() <= 0) {
                throw new OptionValidationException(
                        "chunk_size must be > 0, but was " + config.getChunkSize());
            }
            if (config.getOverlapSize() < 0 || config.getOverlapSize() >= config.getChunkSize()) {
                throw new OptionValidationException(
                        "overlap_size must satisfy 0 <= overlap_size < chunk_size, but was "
                                + config.getOverlapSize()
                                + " (chunk_size="
                                + config.getChunkSize()
                                + ")");
            }
        }
    }
}
